"""Calls and borrower API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.webhook_auth import verify_provider_webhook
from app.persistence.database import get_async_db
from app.persistence.models import Borrower
from app.persistence.repositories import BorrowerRepository, CallRepository
from app.workers.runtime import DialerRuntime, get_runtime

router = APIRouter()


async def get_db():
    async with get_async_db() as session:
        yield session


class BorrowerCreate(BaseModel):
    campaign_id: str
    synthetic_name: str = Field(..., min_length=1, max_length=255)
    masked_phone: str = Field(..., min_length=1, max_length=64)
    priority: int = Field(default=0, ge=0, le=100)
    max_attempts: int = Field(default=3, ge=1, le=10)


class BorrowerResponse(BaseModel):
    id: str
    campaign_id: str
    synthetic_name: str
    masked_phone: str
    status: str
    attempt_count: int
    priority: int

    model_config = {"from_attributes": True}


@router.post("/borrowers", response_model=BorrowerResponse, status_code=201)
async def register_borrower(
    body: BorrowerCreate,
    session: AsyncSession = Depends(get_db),
):
    from app.persistence.repositories import CampaignRepository
    camp_repo = CampaignRepository(session)
    campaign = await camp_repo.get(body.campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail="Campaign not found")

    repo = BorrowerRepository(session)
    borrower = Borrower(
        campaign_id=body.campaign_id,
        synthetic_name=body.synthetic_name,
        masked_phone=body.masked_phone,
        priority=body.priority,
        max_attempts=body.max_attempts,
        status="QUEUED",
    )
    borrower = await repo.create(borrower)
    return borrower


@router.get("/campaigns/{campaign_id}/borrowers")
async def list_campaign_borrowers(
    campaign_id: str,
    limit: int = 100,
    session: AsyncSession = Depends(get_db),
):
    repo = BorrowerRepository(session)
    borrowers = await repo.list_by_campaign(campaign_id, limit=limit)
    return [
        {
            "id": b.id,
            "synthetic_name": b.synthetic_name,
            "status": b.status,
            "attempt_count": b.attempt_count,
            "priority": b.priority,
        }
        for b in borrowers
    ]


@router.get("/campaigns/{campaign_id}/calls")
async def list_campaign_calls(
    campaign_id: str,
    limit: int = 100,
    session: AsyncSession = Depends(get_db),
):
    repo = CallRepository(session)
    calls = await repo.list_by_campaign(campaign_id, limit=limit)
    return [
        {
            "id": c.id,
            "state": c.state,
            "agent_id": c.agent_id,
            "borrower_id": c.borrower_id,
            "provider_call_id": c.provider_call_id,
            "is_ambiguous": c.is_ambiguous,
            "attempt_number": c.attempt_number,
            "created_at": c.created_at.isoformat(),
        }
        for c in calls
    ]


@router.get("/calls/{call_id}")
async def get_call(
    call_id: str,
    session: AsyncSession = Depends(get_db),
):
    from app.persistence.repositories import DispatchRepository

    repo = CallRepository(session)
    call = await repo.get(call_id)
    if call is None:
        raise HTTPException(status_code=404, detail="Call not found")
    dispatch = await DispatchRepository(session).get_by_call_id(call_id)
    return {
        "dispatch_status": dispatch.status if dispatch else None,
        "dispatch_result_code": dispatch.result_code if dispatch else None,
        "dispatch_attempts": dispatch.attempt_count if dispatch else None,
        "id": call.id,
        "state": call.state,
        "campaign_id": call.campaign_id,
        "agent_id": call.agent_id,
        "borrower_id": call.borrower_id,
        "provider_id": call.provider_id,
        "provider_call_id": call.provider_call_id,
        "is_ambiguous": call.is_ambiguous,
        "attempt_number": call.attempt_number,
        "failure_reason": call.failure_reason,
        "idempotency_key": call.idempotency_key,
        "created_at": call.created_at.isoformat(),
        "initiated_at": call.initiated_at.isoformat() if call.initiated_at else None,
        "answered_at": call.answered_at.isoformat() if call.answered_at else None,
        "connected_at": call.connected_at.isoformat() if call.connected_at else None,
        "completed_at": call.completed_at.isoformat() if call.completed_at else None,
    }


@router.post("/provider-events")
async def receive_provider_event(
    request: Request,
    provider_id: str,
    provider_event_id: str,
    provider_call_id: str,
    event_type: str,
    payload: dict = None,
    session: AsyncSession = Depends(get_db),
):
    """Receive a provider webhook event and process it idempotently.

    Authenticated with a per-provider HMAC signature when
    PROVIDER_WEBHOOK_SECRETS is set (see app/api/webhook_auth.py).
    """
    from app.persistence.retry import run_transaction
    from app.services.event_processor import EventProcessor

    await verify_provider_webhook(request, provider_id)
    processor = EventProcessor()

    async def process(s: AsyncSession) -> bool:
        return await processor.process_raw(
            s,
            provider_id=provider_id,
            provider_event_id=provider_event_id,
            provider_call_id=provider_call_id,
            event_type=event_type,
            payload=payload or {},
        )

    changed = await run_transaction(session, process, op="provider_webhook")
    return {"processed": True, "state_changed": changed}


@router.post("/reconcile")
async def run_reconciliation(
    session: AsyncSession = Depends(get_db),
    runtime: DialerRuntime = Depends(get_runtime),
):
    """Trigger reconciliation manually (it also runs periodically in the background)."""
    results = await runtime.reconciler.run(session)
    return results
