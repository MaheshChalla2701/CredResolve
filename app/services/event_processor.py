"""Durable, idempotent event processor.

Event processing pipeline:
  1. Receive raw event (from provider webhook or the mock provider queue).
  2. Persist to provider_event_inbox — ON CONFLICT DO NOTHING (deduplication).
  3. If duplicate → return immediately (no state change).
  4. Find and lock the call row (SELECT FOR UPDATE, refreshed from the database).
  5. Work out the transition path from the current call state.
  6. Apply the call transition(s), then the agent/borrower consequences
     (app.services.call_outcomes) in the same transaction.
  7. Mark the inbox record PROCESSED and write an audit event.

Ordering rules (sequence numbers are recorded but state-machine rules decide):
  - Skip-ahead events are applied through their implied steps, e.g. CONNECTED
    arriving while the call is RINGING applies RINGING → ANSWERED → CONNECTED.
  - Regressive events (RINGING after ANSWERED, ANSWERED after CONNECTED) are
    stale: recorded in the inbox and skipped.
  - Once a call is terminal its state never changes. A late ANSWERED/CONNECTED
    event only back-fills the missing answered_at/connected_at timestamp so the
    facts (e.g. "this call did connect") are not lost.
  - An event whose provider_call_id is not recorded yet (the provider can emit
    events before the dispatch worker commits the id) stays PENDING in the inbox
    and is retried by the runtime (retry_unmatched).

Outcome mapping:
  completed            → call COMPLETED (borrower was reached)
  failed / busy        → call FAILED, failure_reason = event type
  no_answer            → call FAILED, failure_reason = "no_answer" (retryable)
  cancelled            → call CANCELLED
"""
from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import update

from app.core.enums import (
    TERMINAL_CALL_STATES,
    VALID_CALL_TRANSITIONS,
    AgentState,
    AuditEventType,
    CallState,
    ProviderEventType,
)
from app.core.errors import StaleUpdateError
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
from app.core.timeutils import utcnow
from app.domain.state_machines import is_terminal_call_state
from app.persistence.models import AuditEvent, Call, ProviderEventInbox
from app.persistence.repositories import (
    AuditRepository,
    CallRepository,
    ProviderEventRepository,
)
from app.services.call_outcomes import (
    call_was_contacted,
    move_agent_for_call,
    release_agent_after_call,
    settle_borrower,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


# Map provider event types → call state transitions
_EVENT_TO_STATE: dict[str, str] = {
    ProviderEventType.RINGING: CallState.RINGING,
    ProviderEventType.ANSWERED: CallState.ANSWERED,
    ProviderEventType.CONNECTED: CallState.CONNECTED,
    ProviderEventType.COMPLETED: CallState.COMPLETED,
    ProviderEventType.FAILED: CallState.FAILED,
    ProviderEventType.CANCELLED: CallState.CANCELLED,
    ProviderEventType.NO_ANSWER: CallState.FAILED,   # unanswered attempt — retryable
    ProviderEventType.BUSY: CallState.FAILED,
}

# Events that carry no state change (the dispatch worker already set INITIATED)
_INFORMATIONAL_EVENTS = frozenset({ProviderEventType.INITIATED})

# Agent state while the call is in progress
_AGENT_STATE_FOR_CALL: dict[str, str] = {
    CallState.RINGING: AgentState.DIALING,
    CallState.ANSWERED: AgentState.DIALING,
    CallState.CONNECTED: AgentState.CONNECTED,
}


def _call_path(current: str, target: str) -> list[str] | None:
    """Transition path for an event, or None if the event is stale/regressive."""
    if (CallState(current), CallState(target)) in VALID_CALL_TRANSITIONS:
        return [target]
    # CONNECTED implies the call was answered: fast-forward through ANSWERED.
    if (
        target == CallState.CONNECTED
        and (CallState(current), CallState.ANSWERED) in VALID_CALL_TRANSITIONS
    ):
        return [CallState.ANSWERED, CallState.CONNECTED]
    return None


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

        Returns True if the event produced a state change; False if duplicate,
        stale, or still waiting for its call to be matched.

        This method is safe to call multiple times with the same event.
        """
        t0 = time.perf_counter()
        events_received.labels(provider=provider_id, event_type=event_type).inc()

        inbox_entry = ProviderEventInbox(
            id=str(uuid.uuid4()),
            provider_id=provider_id,
            provider_event_id=provider_event_id,
            provider_call_id=provider_call_id,
            event_type=event_type,
            payload=payload,
            sequence_number=sequence_number,
            received_at=utcnow(),
        )
        inbox_entry, is_new = await ProviderEventRepository(session).insert_if_new(inbox_entry)
        if not is_new:
            events_deduplicated.labels(provider=provider_id).inc()
            logger.debug(
                "event_duplicate_suppressed",
                provider=provider_id,
                event_id=provider_event_id,
                call_id=provider_call_id,
            )
            return False

        changed = await self._apply(
            session,
            inbox_id=inbox_entry.id,
            provider_id=provider_id,
            provider_event_id=provider_event_id,
            provider_call_id=provider_call_id,
            event_type=event_type,
        )
        event_processing_latency.labels(provider=provider_id).observe(time.perf_counter() - t0)
        return changed

    async def retry_unmatched(
        self,
        session: "AsyncSession",
        *,
        inbox_id: str,
        provider_id: str,
        provider_event_id: str,
        provider_call_id: str,
        event_type: str,
        give_up: bool,
    ) -> bool:
        """Re-apply an inbox event that arrived before its provider_call_id was
        recorded. With give_up=True an event that still has no call is closed."""
        found = await CallRepository(session).get_by_provider_call_id(provider_call_id)
        if found is None:
            if give_up:
                logger.warning(
                    "event_no_matching_call",
                    provider_call_id=provider_call_id,
                    event_type=event_type,
                )
                await ProviderEventRepository(session).mark_processed(
                    inbox_id, error="NO_MATCHING_CALL"
                )
            return False
        return await self._apply(
            session,
            inbox_id=inbox_id,
            provider_id=provider_id,
            provider_event_id=provider_event_id,
            provider_call_id=provider_call_id,
            event_type=event_type,
        )

    async def _apply(
        self,
        session: "AsyncSession",
        *,
        inbox_id: str,
        provider_id: str,
        provider_event_id: str,
        provider_call_id: str,
        event_type: str,
    ) -> bool:
        event_repo = ProviderEventRepository(session)
        call_repo = CallRepository(session)

        found = await call_repo.get_by_provider_call_id(provider_call_id)
        if found is None:
            logger.info(
                "event_awaiting_call_match",
                provider_call_id=provider_call_id,
                event_type=event_type,
            )
            await event_repo.note_unmatched(inbox_id)
            return False

        call = await call_repo.get_for_update(found.id)
        current_state = call.state

        if event_type in _INFORMATIONAL_EVENTS:
            await event_repo.mark_processed(inbox_id)
            return False

        target_state = _EVENT_TO_STATE.get(event_type)
        if target_state is None:
            logger.warning("unknown_event_type", event_type=event_type)
            await event_repo.mark_processed(inbox_id, error=f"UNKNOWN_EVENT_TYPE:{event_type}")
            return False

        if is_terminal_call_state(current_state):
            events_out_of_order.labels(provider=provider_id).inc()
            await self._backfill_late_facts(session, call, target_state)
            logger.info(
                "event_dropped_terminal_call",
                call_id=call.id,
                current_state=current_state,
                event_type=event_type,
                provider_event_id=provider_event_id,
            )
            await event_repo.mark_processed(
                inbox_id, error=f"CALL_ALREADY_TERMINAL:{current_state}"
            )
            return False

        path = _call_path(current_state, target_state)
        if path is None:
            events_out_of_order.labels(provider=provider_id).inc()
            logger.info(
                "event_stale",
                call_id=call.id,
                current_state=current_state,
                target_state=target_state,
                event_type=event_type,
            )
            await event_repo.mark_processed(
                inbox_id, error=f"STALE_EVENT:{current_state}->{target_state}"
            )
            return False
        if len(path) > 1:
            events_out_of_order.labels(provider=provider_id).inc()
            logger.info("event_fast_forward", call_id=call.id, path=[current_state, *path])

        # ── Apply the call transition(s) ──────────────────────────────────
        state, version = current_state, call.state_version
        for step in path:
            extra: dict = {}
            now = utcnow()
            if step == CallState.ANSWERED:
                extra["answered_at"] = now
            elif step == CallState.CONNECTED:
                extra["connected_at"] = now
            elif step in TERMINAL_CALL_STATES:
                extra["completed_at"] = now
                if step != CallState.COMPLETED:
                    extra["failure_reason"] = event_type
            updated = await call_repo.transition_state(
                call.id, from_state=state, to_state=step, version=version, **extra
            )
            if not updated:
                # We hold the row lock, so this indicates a real concurrency bug;
                # raise so the transaction (including the inbox row) rolls back
                # and the event can be redelivered rather than silently lost.
                raise StaleUpdateError(f"Call {call.id} changed while locked ({state} -> {step})")
            self._emit_metrics(call.campaign_id, step, event_type)
            state, version = step, version + 1

        # ── Agent and borrower consequences ───────────────────────────────
        call = await call_repo.get_for_update(call.id)
        if state in TERMINAL_CALL_STATES:
            contacted = call_was_contacted(call, final_state=state)
            await release_agent_after_call(session, call, contacted=contacted)
            await settle_borrower(session, call, contacted=contacted)
        else:
            await move_agent_for_call(session, call, _AGENT_STATE_FOR_CALL[state])

        await event_repo.mark_processed(inbox_id)
        await AuditRepository(session).log(AuditEvent(
            event_type=AuditEventType.CALL_STATE_CHANGED,
            entity_type="call",
            entity_id=call.id,
            campaign_id=call.campaign_id,
            payload={
                "from_state": current_state,
                "to_state": state,
                "provider_event_id": provider_event_id,
                "event_type": event_type,
            },
        ))
        logger.info(
            "call_state_changed",
            call_id=call.id,
            campaign_id=call.campaign_id,
            from_state=current_state,
            to_state=state,
            event_type=event_type,
        )
        return True

    @staticmethod
    async def _backfill_late_facts(session: "AsyncSession", call: Call, target_state: str) -> None:
        """Record a late answered/connected fact on a terminal call without
        changing its state (only fills timestamps that are still empty)."""
        column = {
            CallState.ANSWERED: Call.answered_at,
            CallState.CONNECTED: Call.connected_at,
        }.get(target_state)
        if column is None:
            return
        await session.execute(
            update(Call)
            .where(Call.id == call.id, column.is_(None))
            .values({column.key: utcnow()})
        )

    @staticmethod
    def _emit_metrics(campaign_id: str, state: str, event_type: str) -> None:
        if state == CallState.ANSWERED:
            calls_answered.labels(campaign_id=campaign_id).inc()
        elif state == CallState.CONNECTED:
            calls_connected.labels(campaign_id=campaign_id).inc()
        elif state == CallState.COMPLETED:
            calls_completed.labels(campaign_id=campaign_id).inc()
        elif state == CallState.FAILED:
            calls_failed.labels(campaign_id=campaign_id, reason=event_type).inc()
