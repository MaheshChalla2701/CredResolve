"""Campaign API endpoints."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.database import get_async_db
from app.persistence.models import Campaign
from app.persistence.repositories import CampaignRepository

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
):
    """Run one dialing cycle for the campaign using its configured mode."""
    from app.services.call_allocator import CallAllocator
    from app.services.safety_controller import SafetyController
    from app.services.progressive_dialer import ProgressiveDialer
    from app.persistence.repositories import CampaignRepository

    repo = CampaignRepository(session)
    c = await repo.get(campaign_id)
    if c is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if c.status != "ACTIVE":
        raise HTTPException(status_code=400, detail="Campaign must be ACTIVE to dial")

    safety = SafetyController()
    allocator = CallAllocator(provider_id=provider_id)
    dialer = ProgressiveDialer(safety, allocator)

    calls, decision = await dialer.run_cycle(
        session, campaign_id, provider_id
    )

    return {
        "campaign_id": campaign_id,
        "calls_allocated": len(calls),
        "call_ids": [c.id for c in calls],
        "safety_decision": {
            "decision": decision.decision,
            "approved_count": decision.approved_count,
            "reason_codes": decision.reason_codes,
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
            "reasons": d.reason_codes,
            "decided_at": d.decided_at.isoformat(),
        }
        for d in decisions
    ]
