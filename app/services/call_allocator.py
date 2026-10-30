"""Atomic Call Allocator — the ONLY component allowed to create call reservations.

Implements:
  1. Campaign row lock (CampaignRepository.lock_for_dialing): the campaign must
     be ACTIVE and below max_concurrent_calls, checked inside the same
     transaction that creates the call, so concurrent dialing cycles cannot
     jointly exceed the cap.
  2. SELECT ... FOR UPDATE SKIP LOCKED on agent row.
  3. SELECT ... FOR UPDATE SKIP LOCKED on borrower row.
  4. Atomic creation of: Call record, Agent state update,
     Borrower status update, DispatchRecord (outbox).
  5. All written in a single database transaction.
  6. An idempotency key is derived from campaign + borrower + attempt_number
     so that duplicate allocation attempts are safe.

The hard limits above hold for every caller. The policy checks that need
provider health (circuit breaker, half-open probes) live in the Safety
Controller; only the dialers in app/services/dialing.py call the allocator, and
always with a count the Safety Controller approved.

Trade-off of the campaign lock: allocation is serialized per campaign (not
across campaigns). A dialing cycle holds it for a few milliseconds; a counter
column decremented on every terminal transition would avoid the lock but must
be kept exact on every code path that finishes a call.

Why SKIP LOCKED?
  - Multiple concurrent workers compete for available agents/borrowers.
  - SKIP LOCKED allows each worker to pick a different row without blocking.
  - Trade-off: a worker sees fewer rows than exist (some are locked by peers).
    This is intentional — it prevents pile-up at the same row.
  - Alternative (NOWAIT) would raise an exception on conflict; SKIP LOCKED
    gracefully moves on to the next candidate.

Why NOT a Python asyncio lock?
  - asyncio locks only protect within a single process.
  - With multiple workers or machines, there is no shared asyncio lock.
  - Database transactions with row-level locks are the correct primitive.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from app.core.config import get_settings
from app.core.enums import AgentState, CallState, DispatchStatus
from app.core.errors import (
    AllocationError,
    CampaignConcurrencyLimitError,
    CampaignNotActiveError,
    NoAvailableAgentError,
    NoEligibleBorrowerError,
)
from app.core.logging import get_logger
from app.core.metrics import allocation_latency, calls_approved
from app.domain.state_machines import validate_agent_transition
from app.persistence.models import Call, DispatchRecord
from app.persistence.repositories import (
    AgentRepository,
    BorrowerRepository,
    CallRepository,
    CampaignRepository,
    DispatchRepository,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
_settings = get_settings()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _make_idempotency_key(
    campaign_id: str, borrower_id: str, attempt_number: int
) -> str:
    """Deterministic idempotency key for a logical call attempt."""
    return f"alloc:{campaign_id}:{borrower_id}:{attempt_number}"


@dataclass
class BatchAllocation:
    """Calls reserved by allocate_up_to(), and why it stopped short (if it did)."""
    calls: list["Call"] = field(default_factory=list)
    stop_reason: str | None = None


class CallAllocator:
    """Performs atomic (agent, borrower, call, outbox) reservation.

    This is the ONLY component authorized to:
      - Transition an agent from AVAILABLE → RESERVED.
      - Transition a borrower from QUEUED → ALLOCATED.
      - Create a Call record.
      - Write a DispatchRecord (outbox entry).

    All four operations happen inside a single database transaction.
    No provider calls are made here — the dispatch worker reads the outbox.
    """

    def __init__(self, provider_id: str = "mock_provider_a") -> None:
        self.provider_id = provider_id

    async def allocate_one(
        self,
        session: "AsyncSession",
        campaign_id: str,
    ) -> "Call":
        """Reserve one agent + borrower and create a call record atomically.

        Raises:
          CampaignNotActiveError: if the campaign is missing or not ACTIVE.
          CampaignConcurrencyLimitError: if max_concurrent_calls are active.
          NoAvailableAgentError: if no AVAILABLE agent can be claimed.
          NoEligibleBorrowerError: if no QUEUED borrower is available.
          AllocationError: on any other allocation failure.

        The session must NOT be committed inside this method — the caller
        is responsible for committing (so they can batch multiple operations).
        """
        t0 = time.perf_counter()

        agent_repo = AgentRepository(session)
        borrower_repo = BorrowerRepository(session)
        call_repo = CallRepository(session)
        dispatch_repo = DispatchRepository(session)

        # ── Step 0: Campaign lock + hard limits (same transaction) ────────
        await self._check_campaign_limits(session, campaign_id)

        # ── Step 1: Claim an AVAILABLE agent (row-level lock) ─────────────
        agent = await agent_repo.claim_available_for_update(campaign_id)
        if agent is None:
            raise NoAvailableAgentError(f"No available agent for campaign {campaign_id}")

        # ── Step 2: Claim an eligible QUEUED borrower (row-level lock) ────
        borrower = await borrower_repo.claim_queued_for_update(campaign_id)
        if borrower is None:
            # Release is implicit — transaction will roll back or caller can handle
            raise NoEligibleBorrowerError(
                f"No eligible borrower for campaign {campaign_id}"
            )

        # ── Step 3: Validate state-machine transitions ────────────────────
        validate_agent_transition(agent.state, AgentState.RESERVED)

        # ── Step 4: Calculate attempt number ─────────────────────────────
        attempt_number = (borrower.attempt_count or 0) + 1
        idempotency_key = _make_idempotency_key(
            campaign_id, borrower.id, attempt_number
        )

        # Check for duplicate (belt-and-suspenders; unique constraint is the real guard)
        existing = await call_repo.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            logger.warning(
                "duplicate_allocation_attempted",
                campaign_id=campaign_id,
                borrower_id=borrower.id,
                idempotency_key=idempotency_key,
                existing_call_id=existing.id,
            )
            raise AllocationError(
                f"Duplicate allocation for key {idempotency_key}"
            )

        # ── Step 5: Mark borrower ALLOCATED (conditional on still QUEUED) ──
        # The conditional update is the real guard on backends without row
        # locks (SQLite); on PostgreSQL the FOR UPDATE lock already holds it.
        if not await borrower_repo.mark_allocated(borrower.id):
            raise AllocationError(
                f"Borrower {borrower.id} was allocated concurrently"
            )

        # ── Step 6: Create the Call record ────────────────────────────────
        call_id = str(uuid.uuid4())
        call = Call(
            id=call_id,
            campaign_id=campaign_id,
            agent_id=agent.id,
            borrower_id=borrower.id,
            provider_id=self.provider_id,
            state=CallState.RESERVED,
            idempotency_key=idempotency_key,
            attempt_number=attempt_number,
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
        call = await call_repo.create(call)

        # ── Step 7: Transition agent AVAILABLE → RESERVED ─────────────────
        updated = await agent_repo.transition_state(
            agent.id,
            from_state=AgentState.AVAILABLE,
            to_state=AgentState.RESERVED,
            version=agent.version,
            current_call_id=call_id,
        )
        if not updated:
            # Undo this allocation's writes: allocate_batch stops on this error
            # but the caller still commits the calls allocated before it.
            await session.delete(call)
            await session.flush()
            await borrower_repo.undo_allocation(borrower.id)
            raise AllocationError(
                f"Agent {agent.id} state changed concurrently; allocation undone"
            )

        # ── Step 8: Write dispatch outbox record ──────────────────────────
        dispatch = DispatchRecord(
            call_id=call_id,
            campaign_id=campaign_id,
            provider_id=self.provider_id,
            idempotency_key=idempotency_key,
            masked_phone=borrower.masked_phone,
            status=DispatchStatus.PENDING,
        )
        await dispatch_repo.create(dispatch)

        elapsed = time.perf_counter() - t0
        allocation_latency.labels(campaign_id=campaign_id).observe(elapsed)
        calls_approved.labels(campaign_id=campaign_id).inc()

        logger.info(
            "call_allocated",
            call_id=call_id,
            agent_id=agent.id,
            borrower_id=borrower.id,
            campaign_id=campaign_id,
            attempt=attempt_number,
            elapsed_ms=round(elapsed * 1000, 1),
        )

        return call

    async def _check_campaign_limits(self, session: "AsyncSession", campaign_id: str) -> None:
        campaign = await CampaignRepository(session).lock_for_dialing(campaign_id)
        if campaign is None:
            raise CampaignNotActiveError(campaign_id)
        active = await CallRepository(session).count_active(campaign_id)
        if active >= campaign.max_concurrent_calls:
            raise CampaignConcurrencyLimitError(
                f"Campaign {campaign_id} has {active} active calls "
                f"(max_concurrent_calls={campaign.max_concurrent_calls})"
            )

    async def allocate_up_to(
        self,
        session: "AsyncSession",
        campaign_id: str,
        count: int,
    ) -> BatchAllocation:
        """Allocate up to `count` calls in the caller's transaction.

        Stops at the first failure and reports why, so the caller can record a
        shortfall against the Safety Controller's approval.
        """
        outcome = BatchAllocation()
        for _ in range(count):
            try:
                outcome.calls.append(await self.allocate_one(session, campaign_id))
            except NoAvailableAgentError:
                outcome.stop_reason = "NO_AVAILABLE_AGENT"
            except NoEligibleBorrowerError:
                outcome.stop_reason = "NO_ELIGIBLE_BORROWER"
            except CampaignConcurrencyLimitError:
                outcome.stop_reason = "CAMPAIGN_CONCURRENCY_LIMIT"
            except CampaignNotActiveError:
                outcome.stop_reason = "CAMPAIGN_NOT_ACTIVE"
            except AllocationError as exc:
                logger.warning("allocation_skipped", reason=str(exc))
                outcome.stop_reason = "ALLOCATION_CONFLICT"
            if outcome.stop_reason:
                break
        return outcome

    async def allocate_batch(
        self,
        session: "AsyncSession",
        campaign_id: str,
        count: int,
    ) -> list["Call"]:
        """Allocate up to `count` calls. Returns fewer if capacity runs out."""
        return (await self.allocate_up_to(session, campaign_id, count)).calls
