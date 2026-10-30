"""Predictive Dialer — pacing-engine proposal, Safety Controller decision.

One cycle:
  1. Rebuild the answer-rate estimate from the campaign's recent call outcomes.
  2. Read provider health and turn it into a pacing factor.
  3. Ask the PredictivePacingEngine for an advisory dial count, capped by
     the number of eligible borrowers.
  4. Submit that count to the Safety Controller (the only authority).
  5. Run the Atomic Allocator up to the approved count and record the
     allocated count on the safety decision.

It does NOT call providers: allocated calls go to the dispatch outbox like the
progressive path. In strict safety mode (the default) every call still needs a
reserved AVAILABLE agent, so pacing can only ever dial at or below one call per
available agent — it reduces dialing when the answer-rate estimate is
uncertain, the provider is degraded, or many calls are already in flight.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.core.enums import SafetyDecision
from app.core.logging import get_logger
from app.persistence.repositories import BorrowerRepository
from app.services.call_allocator import CallAllocator
from app.services.predictive_pacer import PacingRecommendation, PredictivePacingEngine
from app.services.provider_health import provider_health_factor
from app.services.safety_controller import SafetyController, SafetyDecisionResult

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from app.services.provider_health import ProviderHealthService

logger = get_logger(__name__)


class PredictiveDialer:
    def __init__(
        self,
        safety_controller: SafetyController,
        allocator: CallAllocator,
        pacer: PredictivePacingEngine | None = None,
    ) -> None:
        self._safety = safety_controller
        self._allocator = allocator
        self._pacer = pacer or PredictivePacingEngine()

    async def run_cycle(
        self,
        session: "AsyncSession",
        campaign_id: str,
        provider_id: str,
        *,
        health_service: "ProviderHealthService | None" = None,
        max_batch: int = 50,
    ) -> tuple[list, SafetyDecisionResult, PacingRecommendation]:
        """Returns (calls_created, safety_decision, pacing_recommendation)."""
        await self._pacer.refresh_answer_rate(session, campaign_id)

        health_factor = 1.0
        if health_service is not None:
            health_factor = provider_health_factor(await health_service.get_snapshot(session))

        rec = await self._pacer.recommend(
            session, campaign_id, provider_id, provider_health_factor=health_factor
        )
        eligible = await BorrowerRepository(session).count_eligible(campaign_id)
        proposed = min(rec.recommended_count, max_batch, eligible)

        if proposed <= 0:
            # Nothing proposed — mirrors ProgressiveDialer, which also skips the
            # safety check (and its audit record) when there is nothing to approve.
            reason = "PACER_RECOMMENDED_ZERO" if rec.recommended_count <= 0 else "NO_ELIGIBLE_BORROWERS"
            return [], SafetyDecisionResult(
                campaign_id=campaign_id,
                requested_count=0,
                approved_count=0,
                decision=SafetyDecision.REJECT,
                reason_codes=[reason],
            ), rec

        decision = await self._safety.evaluate(
            session,
            campaign_id=campaign_id,
            requested_count=proposed,
            provider_id=provider_id,
        )
        if decision.decision == SafetyDecision.REJECT or decision.approved_count == 0:
            logger.info("predictive_cycle_rejected", campaign_id=campaign_id, reasons=decision.reason_codes)
            return [], decision, rec

        outcome = await self._allocator.allocate_up_to(session, campaign_id, decision.approved_count)
        calls = outcome.calls
        await self._safety.record_allocation(
            session, decision, allocated=len(calls), stop_reason=outcome.stop_reason
        )
        logger.info(
            "predictive_cycle_complete",
            campaign_id=campaign_id,
            recommended=rec.recommended_count,
            approved=decision.approved_count,
            allocated=len(calls),
            answer_rate=round(rec.answer_rate_estimate, 3),
            health_factor=health_factor,
        )
        return calls, decision, rec
