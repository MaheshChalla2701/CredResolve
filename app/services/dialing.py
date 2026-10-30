"""Campaign dialing cycle — selects the dialer for the campaign's configured mode.

Both modes end in the same place: SafetyController.evaluate() decides the
approved count, and only then does the CallAllocator reserve agents and
borrowers and write dispatch outbox records. Neither mode contacts a provider;
the dispatch worker does that.

This is the only entry point that creates calls (POST /dial and the simulation
use it). The whole cycle runs under the campaign lock, taken first: proposal,
safety decision and allocation all see the same capacity, and a concurrent
cycle for the same campaign waits until this transaction commits. The caller
owns the transaction and must commit or roll back promptly.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.core.enums import DialingMode
from app.persistence.repositories import CampaignRepository
from app.services.call_allocator import CallAllocator
from app.services.predictive_dialer import PredictiveDialer
from app.services.progressive_dialer import ProgressiveDialer

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from app.persistence.models import Call, Campaign
    from app.services.predictive_pacer import PacingRecommendation
    from app.services.provider_health import ProviderHealthService
    from app.services.safety_controller import SafetyController, SafetyDecisionResult


@dataclass
class DialCycleResult:
    mode: str
    calls: list["Call"]
    decision: "SafetyDecisionResult"
    recommendation: "PacingRecommendation | None" = None


async def run_dialing_cycle(
    session: "AsyncSession",
    campaign: "Campaign",
    *,
    provider_id: str,
    safety: "SafetyController",
    health_service: "ProviderHealthService | None" = None,
) -> DialCycleResult:
    """Run one dialing cycle for the campaign using its configured dialing_mode."""
    allocator = CallAllocator(provider_id=provider_id)
    # Serialize with other dialing cycles for this campaign before reading any
    # capacity. If the campaign is no longer ACTIVE nothing is locked; the
    # Safety Controller then rejects (and records) the request.
    await CampaignRepository(session).lock_for_dialing(campaign.id)

    if campaign.dialing_mode == DialingMode.PREDICTIVE:
        calls, decision, rec = await PredictiveDialer(safety, allocator).run_cycle(
            session, campaign.id, provider_id, health_service=health_service
        )
        return DialCycleResult(DialingMode.PREDICTIVE, calls, decision, rec)

    if campaign.dialing_mode == DialingMode.PROGRESSIVE:
        calls, decision = await ProgressiveDialer(safety, allocator).run_cycle(
            session, campaign.id, provider_id
        )
        return DialCycleResult(DialingMode.PROGRESSIVE, calls, decision)

    raise ValueError(f"Unsupported dialing mode: {campaign.dialing_mode!r}")
