"""Progressive Dialer — deterministic baseline.

Policy: exactly one outbound attempt per available agent slot.
No over-dialing. No predictions. Agents must be AVAILABLE before dialing.

The progressive dialer:
  1. Proposes min(available agents, eligible borrowers, max_batch).
  2. Asks the Safety Controller how many calls can be placed.
  3. Runs the Atomic Allocator up to that many times.
  4. Records the allocated count on the safety decision.
  5. Returns the set of allocated Call records.

It does NOT:
  - Call providers directly.
  - Access provider clients.
  - Predict answer rates.
  - Over-dial beyond verified available agents.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.core.enums import SafetyDecision
from app.core.logging import get_logger
from app.core.metrics import calls_proposed
from app.services.call_allocator import CallAllocator
from app.services.safety_controller import SafetyController, SafetyDecisionResult

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


class ProgressiveDialer:
    """One-call-per-available-agent deterministic dialer.

    Invariant: allocated <= approved_count <= number of AVAILABLE agents at
    the time of the safety check. When run through run_dialing_cycle() the
    check and the allocation happen under the same campaign lock, so a
    concurrent cycle cannot consume the approved capacity in between.
    """

    def __init__(
        self,
        safety_controller: SafetyController,
        allocator: CallAllocator,
    ) -> None:
        self._safety = safety_controller
        self._allocator = allocator

    async def run_cycle(
        self,
        session: "AsyncSession",
        campaign_id: str,
        provider_id: str,
        *,
        max_batch: int = 50,
    ) -> tuple[list, SafetyDecisionResult]:
        """Run one dialing cycle: check safety, allocate up to approved count.

        Returns:
          (calls_created, safety_decision)
        """
        from app.persistence.repositories import AgentRepository, BorrowerRepository

        # Count available agents to propose
        agent_repo = AgentRepository(session)
        state_counts = await agent_repo.count_by_state(campaign_id)
        from app.core.enums import AgentState
        available = state_counts.get(AgentState.AVAILABLE, 0)
        # Never ask for more calls than there are borrowers to call.
        eligible = await BorrowerRepository(session).count_eligible(campaign_id)

        proposed = min(available, eligible, max_batch)
        calls_proposed.labels(campaign_id=campaign_id, mode="progressive").inc(proposed)

        if proposed == 0:
            reason = "NO_AVAILABLE_AGENTS" if available == 0 else "NO_ELIGIBLE_BORROWERS"
            logger.debug("progressive_nothing_to_dial", campaign_id=campaign_id, reason=reason)
            # Nothing was proposed, so the Safety Controller is not consulted
            # and no audit record is written (persisted=False).
            trivial = SafetyDecisionResult(
                campaign_id=campaign_id,
                requested_count=0,
                approved_count=0,
                decision=SafetyDecision.REJECT,
                reason_codes=[reason],
            )
            return [], trivial

        # Safety check
        decision = await self._safety.evaluate(
            session,
            campaign_id=campaign_id,
            requested_count=proposed,
            provider_id=provider_id,
        )

        if decision.decision == SafetyDecision.REJECT or decision.approved_count == 0:
            logger.info(
                "progressive_cycle_rejected",
                campaign_id=campaign_id,
                reasons=decision.reason_codes,
            )
            return [], decision

        # Allocate — in same session/transaction
        outcome = await self._allocator.allocate_up_to(
            session, campaign_id, decision.approved_count
        )
        calls = outcome.calls
        await self._safety.record_allocation(
            session, decision, allocated=len(calls), stop_reason=outcome.stop_reason
        )

        logger.info(
            "progressive_cycle_complete",
            campaign_id=campaign_id,
            proposed=proposed,
            approved=decision.approved_count,
            allocated=len(calls),
        )
        return calls, decision
