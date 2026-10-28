"""Progressive Dialer — deterministic baseline.

Policy: exactly one outbound attempt per available agent slot.
No over-dialing. No predictions. Agents must be AVAILABLE before dialing.

The progressive dialer:
  1. Asks the Safety Controller how many calls can be placed.
  2. Runs the Atomic Allocator up to that many times.
  3. Returns the set of allocated Call records.

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

    Invariant: approved_count <= number of AVAILABLE agents at the time
    of the safety check. The safety controller re-reads DB state, so
    concurrent allocations between the check and the allocation will
    cause allocate_one to find fewer available agents and stop early.
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
        from app.persistence.repositories import AgentRepository

        # Count available agents to propose
        agent_repo = AgentRepository(session)
        state_counts = await agent_repo.count_by_state(campaign_id)
        from app.core.enums import AgentState
        available = state_counts.get(AgentState.AVAILABLE, 0)

        proposed = min(available, max_batch)
        calls_proposed.labels(campaign_id=campaign_id, mode="progressive").inc(proposed)

        if proposed == 0:
            logger.debug("progressive_no_agents_available", campaign_id=campaign_id)
            trivial = SafetyDecisionResult(
                campaign_id=campaign_id,
                requested_count=0,
                approved_count=0,
                decision=SafetyDecision.REJECT,
                reason_codes=["NO_AVAILABLE_AGENTS"],
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
        calls = await self._allocator.allocate_batch(
            session, campaign_id, decision.approved_count
        )

        logger.info(
            "progressive_cycle_complete",
            campaign_id=campaign_id,
            proposed=proposed,
            approved=decision.approved_count,
            allocated=len(calls),
        )
        return calls, decision
