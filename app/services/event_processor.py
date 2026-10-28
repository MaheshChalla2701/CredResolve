"""Durable, idempotent event processor.

Event processing pipeline:
  1. Receive raw event (from provider webhook or poll).
  2. Persist to provider_event_inbox — ON CONFLICT DO NOTHING (deduplication).
  3. If duplicate → log, return immediately (no state change).
  4. Lock the relevant call row (SELECT FOR UPDATE).
  5. Validate transition against current call state.
  6. Apply state transition + agent state changes atomically.
  7. Mark inbox record as PROCESSED.
  8. Record audit event.

Out-of-order handling:
  - Each event is validated against the current call state.
  - If the incoming event would regress a terminal state, it is silently
    dropped (logged as out-of-order).
  - Sequence numbers are used when available; otherwise we use state-machine
    rules to determine applicability.
  - A COMPLETED event arriving before ANSWERED: applied immediately.
    If a subsequent ANSWERED event arrives, it finds state=COMPLETED and
    is rejected by the state machine (COMPLETED has no outgoing transitions).

A late ANSWERED event for a COMPLETED call is recorded in the inbox
(for auditability) but produces no state change.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from app.core.enums import (
    TERMINAL_CALL_STATES,
    AgentState,
    AuditEventType,
    BorrowerStatus,
    CallState,
    ProviderEventType,
)
from app.core.errors import InvalidStateTransitionError
from app.core.logging import get_logger
from app.core.metrics import (
    calls_answered,
    calls_completed,
    calls_connected,
    calls_failed,
    event_processing_latency,
    events_deduplicated,
    events_out_of_order,
    events_received,
)
from app.domain.state_machines import is_terminal_call_state, validate_call_transition
from app.persistence.models import AuditEvent, ProviderEventInbox

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# Map provider event types → call state transitions
_EVENT_TO_STATE: dict[str, str] = {
    ProviderEventType.RINGING: CallState.RINGING,
    ProviderEventType.ANSWERED: CallState.ANSWERED,
    ProviderEventType.CONNECTED: CallState.CONNECTED,
    ProviderEventType.COMPLETED: CallState.COMPLETED,
    ProviderEventType.FAILED: CallState.FAILED,
    ProviderEventType.CANCELLED: CallState.CANCELLED,
    ProviderEventType.NO_ANSWER: CallState.COMPLETED,   # treat no-answer as completed
    ProviderEventType.BUSY: CallState.FAILED,
}

# Agent state to transition to after a call completes or fails
def _agent_target_state_for_call_end(from_call_state: str) -> str:
    """Determine agent state after a call ends."""
    if from_call_state in (CallState.CONNECTED, CallState.ANSWERED):
        return AgentState.WRAP_UP
    elif from_call_state in (CallState.DIALING, CallState.RINGING):
        return AgentState.WRAP_UP
    else:
        # Pre-dial failure — agent goes straight back to AVAILABLE
        return AgentState.AVAILABLE


class EventProcessor:
    """Idempotent, order-tolerant provider event processor."""

    async def process_raw(
        self,
        session: "AsyncSession",
        *,
        provider_id: str,
        provider_event_id: str,
        provider_call_id: str,
        event_type: str,
        payload: dict,
        sequence_number: int | None = None,
    ) -> bool:
        """Process a raw provider event.

        Returns True if the event produced a state change; False if duplicate
        or inapplicable (out-of-order).

        This method is safe to call multiple times with the same event.
        """
        t0 = time.perf_counter()
        events_received.labels(provider=provider_id, event_type=event_type).inc()

        # ── Step 1: Persist to inbox (deduplicate) ────────────────────────
        import uuid
        inbox_entry = ProviderEventInbox(
            id=str(uuid.uuid4()),
            provider_id=provider_id,
            provider_event_id=provider_event_id,
            provider_call_id=provider_call_id,
            event_type=event_type,
            payload=payload,
            sequence_number=sequence_number,
            received_at=_utcnow(),
        )

        from app.persistence.repositories import ProviderEventRepository
        event_repo = ProviderEventRepository(session)
        inbox_entry, is_new = await event_repo.insert_if_new(inbox_entry)

        if not is_new:
            events_deduplicated.labels(provider=provider_id).inc()
            logger.debug(
                "event_duplicate_suppressed",
                provider=provider_id,
                event_id=provider_event_id,
                call_id=provider_call_id,
            )
            return False

        # ── Step 2: Find the call ─────────────────────────────────────────
        from app.persistence.repositories import CallRepository
        call_repo = CallRepository(session)
        call = await call_repo.get_by_provider_call_id(provider_call_id)

        if call is None:
            logger.warning(
                "event_no_matching_call",
                provider_call_id=provider_call_id,
                event_type=event_type,
            )
            await event_repo.mark_processed(
                inbox_entry.id, error="NO_MATCHING_CALL"
            )
            return False

        # ── Step 3: Lock the call row ─────────────────────────────────────
        call = await call_repo.get_for_update(call.id)
        if call is None:
            return False

        current_state = call.state

        # ── Step 4: Check if already terminal ────────────────────────────
        if is_terminal_call_state(current_state):
            events_out_of_order.labels(provider=provider_id).inc()
            logger.info(
                "event_dropped_terminal_call",
                call_id=call.id,
                current_state=current_state,
                event_type=event_type,
                provider_event_id=provider_event_id,
            )
            await event_repo.mark_processed(
                inbox_entry.id, error=f"CALL_ALREADY_TERMINAL:{current_state}"
            )
            return False

        # ── Step 5: Map event to target state ─────────────────────────────
        target_state = _EVENT_TO_STATE.get(event_type)
        if target_state is None:
            logger.warning("unknown_event_type", event_type=event_type)
            await event_repo.mark_processed(
                inbox_entry.id, error=f"UNKNOWN_EVENT_TYPE:{event_type}"
            )
            return False

        # ── Step 6: Validate state-machine transition ─────────────────────
        try:
            validate_call_transition(current_state, target_state)
        except InvalidStateTransitionError:
            events_out_of_order.labels(provider=provider_id).inc()
            logger.info(
                "event_out_of_order",
                call_id=call.id,
                current_state=current_state,
                target_state=target_state,
                event_type=event_type,
            )
            await event_repo.mark_processed(
                inbox_entry.id,
                error=f"INVALID_TRANSITION:{current_state}->{target_state}",
            )
            return False

        # ── Step 7: Build extra values for call update ────────────────────
        extra: dict = {}
        now = _utcnow()
        if target_state == CallState.ANSWERED:
            extra["answered_at"] = now
        elif target_state == CallState.CONNECTED:
            extra["connected_at"] = now
        elif target_state in TERMINAL_CALL_STATES:
            extra["completed_at"] = now

        if event_type in (ProviderEventType.FAILED, ProviderEventType.BUSY):
            extra["failure_reason"] = event_type

        # ── Step 8: Apply call transition (conditional update) ────────────
        updated = await call_repo.transition_state(
            call.id,
            from_state=current_state,
            to_state=target_state,
            version=call.state_version,
            **extra,
        )
        if not updated:
            logger.warning(
                "call_transition_stale",
                call_id=call.id,
                current_state=current_state,
                target_state=target_state,
            )
            await event_repo.mark_processed(
                inbox_entry.id, error="STALE_UPDATE"
            )
            return False

        # ── Step 9: Handle agent state transitions ────────────────────────
        await self._update_agent(
            session, call, current_state, target_state
        )

        # ── Step 10: Handle borrower re-queue on terminal ─────────────────
        if target_state in TERMINAL_CALL_STATES:
            await self._handle_borrower_terminal(
                session, call, target_state, event_type
            )

        # ── Step 11: Mark inbox processed + emit metrics ──────────────────
        await event_repo.mark_processed(inbox_entry.id)

        self._emit_metrics(call.campaign_id, target_state)

        # ── Step 12: Audit ────────────────────────────────────────────────
        from app.persistence.repositories import AuditRepository
        audit_repo = AuditRepository(session)
        await audit_repo.log(AuditEvent(
            event_type=AuditEventType.CALL_STATE_CHANGED,
            entity_type="call",
            entity_id=call.id,
            campaign_id=call.campaign_id,
            payload={
                "from_state": current_state,
                "to_state": target_state,
                "provider_event_id": provider_event_id,
                "event_type": event_type,
            },
        ))

        elapsed = time.perf_counter() - t0
        event_processing_latency.labels(provider=provider_id).observe(elapsed)

        logger.info(
            "call_state_changed",
            call_id=call.id,
            campaign_id=call.campaign_id,
            from_state=current_state,
            to_state=target_state,
            event_type=event_type,
        )
        return True

    async def _update_agent(
        self,
        session: "AsyncSession",
        call,
        call_from: str,
        call_to: str,
    ) -> None:
        """Update agent state following a call state transition."""
        from app.persistence.repositories import AgentRepository
        agent_repo = AgentRepository(session)

        # Load agent with lock
        agent = await agent_repo.get_for_update(call.agent_id)
        if agent is None:
            logger.error("event_agent_not_found", agent_id=call.agent_id)
            return

        agent_from = agent.state
        agent_to: str | None = None

        if call_to == CallState.RINGING and agent_from == AgentState.RESERVED:
            agent_to = AgentState.DIALING
        elif call_to == CallState.CONNECTED and agent_from in (
            AgentState.DIALING, AgentState.RESERVED
        ):
            agent_to = AgentState.CONNECTED
        elif call_to == CallState.ANSWERED and agent_from in (
            AgentState.DIALING, AgentState.RESERVED
        ):
            # Stay in DIALING until CONNECTED; safety reservation still held
            agent_to = AgentState.DIALING
        elif call_to in TERMINAL_CALL_STATES:
            if agent_from in (AgentState.CONNECTED, AgentState.DIALING, AgentState.RESERVED):
                if call_from in (CallState.CONNECTED,):
                    agent_to = AgentState.WRAP_UP
                elif agent_from == AgentState.CONNECTED:
                    agent_to = AgentState.WRAP_UP
                else:
                    agent_to = AgentState.WRAP_UP  # always wrap up for completed calls

        if agent_to is None:
            return

        # Import validation
        from app.domain.state_machines import VALID_AGENT_TRANSITIONS
        if (AgentState(agent_from), AgentState(agent_to)) not in VALID_AGENT_TRANSITIONS:
            logger.warning(
                "agent_transition_skipped",
                agent_id=call.agent_id,
                from_state=agent_from,
                to_state=agent_to,
            )
            return

        clear_call = call_to in TERMINAL_CALL_STATES
        updated = await agent_repo.transition_state(
            call.agent_id,
            from_state=agent_from,
            to_state=agent_to,
            version=agent.version,
            clear_call=clear_call,
        )
        if not updated:
            logger.warning(
                "agent_transition_stale",
                agent_id=call.agent_id,
                from_state=agent_from,
                to_state=agent_to,
            )

    async def _handle_borrower_terminal(
        self,
        session: "AsyncSession",
        call,
        terminal_state: str,
        event_type: str,
    ) -> None:
        """Handle borrower status when call reaches a terminal state."""
        from datetime import timedelta
        from app.persistence.repositories import BorrowerRepository, CampaignRepository

        borrower_repo = BorrowerRepository(session)
        campaign_repo = CampaignRepository(session)

        borrower = await borrower_repo.get(call.borrower_id)
        if borrower is None:
            return

        campaign = await campaign_repo.get(call.campaign_id)
        retry_cooldown = (
            campaign.retry_cooldown_seconds if campaign else _settings.borrower_retry_cooldown_seconds
        )
        max_attempts = (
            campaign.max_attempts_per_borrower if campaign else _settings.max_attempts_per_borrower
        )

        if terminal_state == CallState.COMPLETED:
            # Successfully completed — mark borrower done
            await borrower_repo.update_status(borrower.id, BorrowerStatus.COMPLETED)
        elif borrower.attempt_count >= max_attempts:
            # Exhausted attempts
            await borrower_repo.update_status(borrower.id, BorrowerStatus.EXHAUSTED)
        else:
            # Failed / cancelled — requeue with cooldown
            next_eligible = _utcnow() + timedelta(seconds=retry_cooldown)
            await borrower_repo.update_status(
                borrower.id,
                BorrowerStatus.QUEUED,
                next_eligible_at=next_eligible,
            )

    @staticmethod
    def _emit_metrics(campaign_id: str, state: str) -> None:
        if state == CallState.ANSWERED:
            calls_answered.labels(campaign_id=campaign_id).inc()
        elif state == CallState.CONNECTED:
            calls_connected.labels(campaign_id=campaign_id).inc()
        elif state == CallState.COMPLETED:
            calls_completed.labels(campaign_id=campaign_id).inc()
        elif state == CallState.FAILED:
            calls_failed.labels(campaign_id=campaign_id, reason="provider_event").inc()
