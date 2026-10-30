"""Campaign API endpoints."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.database import get_async_db
from app.persistence.models import Campaign
from app.persistence.repositories import CampaignRepository
from app.workers.runtime import DialerRuntime, get_runtime

router = APIRouter()


class CampaignCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    dialing_mode: str = Field(default="progressive", pattern="^(progressive|predictive)$")
    max_concurrent_calls: int = Field(default=50, ge=1, le=1000)
    max_attempts_per_borrower: int = Field(default=3, ge=1, le=10)
    retry_cooldown_seconds: int = Field(default=3600, ge=0)
    wrap_up_seconds: int = Field(default=30, ge=0, le=600)
    config: dict[str, Any] = Field(default_factory=dict)


class CampaignResponse(BaseModel):
    id: str
    name: str
    status: str
    dialing_mode: str
    max_concurrent_calls: int
    max_attempts_per_borrower: int

    model_config = {"from_attributes": True}


async def get_db():
    async with get_async_db() as session:
        yield session


@router.post("/campaigns", response_model=CampaignResponse, status_code=201)
async def create_campaign(
    body: CampaignCreate,
    session: AsyncSession = Depends(get_db),
):
    repo = CampaignRepository(session)
    campaign = Campaign(
        name=body.name,
        status="DRAFT",
        dialing_mode=body.dialing_mode,
        max_concurrent_calls=body.max_concurrent_calls,
        max_attempts_per_borrower=body.max_attempts_per_borrower,
        retry_cooldown_seconds=body.retry_cooldown_seconds,
        wrap_up_seconds=body.wrap_up_seconds,
        config=body.config,
    )
    campaign = await repo.create(campaign)
    return campaign


@router.get("/campaigns/{campaign_id}", response_model=CampaignResponse)
async def get_campaign(
    campaign_id: str,
    session: AsyncSession = Depends(get_db),
):
    repo = CampaignRepository(session)
    c = await repo.get(campaign_id)
    if c is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return c


@router.post("/campaigns/{campaign_id}/start", response_model=CampaignResponse)
async def start_campaign(
    campaign_id: str,
    session: AsyncSession = Depends(get_db),
):
    repo = CampaignRepository(session)
    c = await repo.get(campaign_id)
    if c is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if c.status not in ("DRAFT", "PAUSED"):
        raise HTTPException(status_code=400, detail=f"Cannot start campaign in status {c.status}")
    await repo.update_status(campaign_id, "ACTIVE")
    c.status = "ACTIVE"
    return c


@router.post("/campaigns/{campaign_id}/pause", response_model=CampaignResponse)
async def pause_campaign(
    campaign_id: str,
    session: AsyncSession = Depends(get_db),
):
    repo = CampaignRepository(session)
    c = await repo.get(campaign_id)
    if c is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if c.status != "ACTIVE":
        raise HTTPException(status_code=400, detail=f"Campaign is not ACTIVE")
    await repo.update_status(campaign_id, "PAUSED")
    c.status = "PAUSED"
    return c


@router.post("/campaigns/{campaign_id}/dial")
async def run_dialing_cycle(
    campaign_id: str,
    provider_id: str = "mock_provider_a",
    session: AsyncSession = Depends(get_db),
    runtime: DialerRuntime = Depends(get_runtime),
):
    """Run one dialing cycle for the campaign using its configured dialing mode.

    Calls are reserved and queued in the dispatch outbox here; the background
    dispatch worker contacts the (mock) provider afterwards. Poll
    GET /api/v1/calls/{id} to follow each call.
    """
    from app.services.dialing import run_dialing_cycle as dial_campaign

    if provider_id not in runtime.providers:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown provider_id {provider_id!r}; available: {sorted(runtime.providers)}",
        )
    repo = CampaignRepository(session)
    c = await repo.get(campaign_id)
    if c is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if c.status != "ACTIVE":
        raise HTTPException(status_code=400, detail="Campaign must be ACTIVE to dial")

    result = await dial_campaign(
        session,
        c,
        provider_id=provider_id,
        safety=runtime.safety_controller(),
        health_service=runtime.health[provider_id],
    )
    # Commit before waking the worker so it can see the new outbox records.
    await session.commit()
    if result.calls:
        runtime.notify_dispatch()

    decision = result.decision
    rec = result.recommendation
    return {
        "campaign_id": campaign_id,
        "dialing_mode": result.mode,
        "calls_allocated": len(result.calls),
        "call_ids": [call.id for call in result.calls],
        # Reserved and queued — not yet placed with the provider.
        "call_state": "RESERVED" if result.calls else None,
        "dispatch_status": "PENDING" if result.calls else None,
        "dispatch_worker_running": runtime.running,
        "safety_decision": {
            "decision_id": decision.decision_id,
            "decision": decision.decision,
            "requested_count": decision.requested_count,
            "approved_count": decision.approved_count,
            "allocated_count": len(result.calls),
            "reason_codes": decision.reason_codes,
            # False when nothing was proposed and the Safety Controller was not consulted.
            "audited": decision.persisted,
        },
        "pacing": None if rec is None else {
            "recommended_count": rec.recommended_count,
            "answer_rate_estimate": round(rec.answer_rate_estimate, 4),
            "answer_rate_confidence": rec.answer_rate_confidence,
            "available_agents": rec.available_agents,
            "outstanding_calls": rec.outstanding_calls,
            "provider_health_factor": rec.provider_health_factor,
            "notes": rec.calculation_notes,
        },
    }


@router.get("/campaigns/{campaign_id}/safety-decisions")
async def get_safety_decisions(
    campaign_id: str,
    limit: int = 20,
    session: AsyncSession = Depends(get_db),
):
    from app.persistence.repositories import SafetyDecisionRepository
    repo = SafetyDecisionRepository(session)
    decisions = await repo.list_recent(campaign_id, limit=limit)
    return [
        {
            "id": d.id,
            "decision": d.decision,
            "requested": d.requested_count,
            "approved": d.approved_count,
            # None when nothing was approved (no allocation attempted), or for
            # records written before allocation tracking existed.
            "allocated": (d.capacity_snapshot or {}).get("allocated_count"),
            "reasons": d.reason_codes,
            "decided_at": d.decided_at.isoformat(),
        }
        for d in decisions
    ]
