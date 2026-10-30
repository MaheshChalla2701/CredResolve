"""Reconciliation service — handles leaks, crashes, and ambiguous outcomes.

Reconciliation is idempotent: running it multiple times is safe. It runs
periodically inside the API process (app.workers.runtime) and on demand via
POST /api/v1/reconcile. Each action is conservative: an agent is only released
when its call is provably finished, and a call is only failed when the provider
provably did not place it (or reports it is no longer active).

Categories handled:
  1. RESERVED calls older than 2× the worker lease:
       no dispatch record / dispatch never claimed → FAILED, agent released,
       borrower retried (the provider was never contacted).
       dispatch CLAIMED with expired lease → flagged ambiguous; the dispatch
       worker re-claims it (CLAIMED proves the provider was not contacted).
  2. SENT dispatch records whose lease expired with no recorded outcome (worker
     died while calling the provider) → call flagged ambiguous.
  3. Ambiguous INITIATED calls → ask the provider whether it accepted the
     idempotency key. Not accepted → FAILED and resources released. Accepted →
     provider_call_id recorded; provider events (or check 4) finish the call.
  4. In-progress calls with no state change for stale_call_seconds → probe the
     provider; if it no longer knows the call, finish it locally.
  5. Agents still RESERVED/DIALING/CONNECTED whose call is finished or missing →
     released (leak recovery).
  6. Agents with stale heartbeats and no active call → OFFLINE.
  7. Agents in WRAP_UP longer than the campaign's wrap_up_seconds → AVAILABLE.

Transactions: each repair is its own short transaction (run_transaction), so a
lock is held only while one call/agent is fixed, never across a provider
lookup, and a failure (or deadlock retry) affects only that item. Rows are
locked in the global order call → dispatch → agent → borrower (see
app/persistence/retry.py), the same order as the dispatch worker and the event
processor, and every condition is re-checked after the lock is taken. run()
commits; callers do not need to.
"""
from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, update

from app.core.config import get_settings
from app.core.enums import (
    TERMINAL_CALL_STATES,
    AgentState,
    CallState,
    DispatchStatus,
)
from app.core.logging import get_logger
from app.core.metrics import reconciliation_actions
from app.core.timeutils import as_utc, utcnow
from app.persistence.models import Agent, Call, Campaign
from app.persistence.repositories import (
    AgentRepository,
    CallRepository,
    DispatchRepository,
)
from app.persistence.retry import run_transaction
from app.services.call_outcomes import (
    call_was_contacted,
    finish_call,
    release_orphaned_agent,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from app.providers.base import ProviderAdapter

logger = get_logger(__name__)
_settings = get_settings()


class ReconciliationService:
    def __init__(
        self,
        provider_adapters: dict[str, "ProviderAdapter"] | None = None,
        *,
        ambiguous_grace_seconds: float | None = None,
    ) -> None:
        self._providers = provider_adapters or {}
        # How long an ambiguous call is left alone before the provider is asked
        # (gives a slow provider time to finish accepting the request).
        self._ambiguous_grace = timedelta(
            seconds=ambiguous_grace_seconds
            if ambiguous_grace_seconds is not None
            else _settings.dispatch_lease_seconds
        )

    async def run(self, session: "AsyncSession") -> dict:
        """Run all reconciliation checks. Returns a summary of actions taken."""
        results: dict = {
            "stale_reserved_resolved": 0,
            "lost_in_flight_flagged": 0,
            "ambiguous_resolved": 0,
            "stale_calls_resolved": 0,
            "leaked_agents_released": 0,
            "stale_agents_offline": 0,
            "wrap_up_released": 0,
            "errors": [],
        }
        steps = [
            ("stale_reserved_resolved", self._resolve_stale_reserved),
            ("lost_in_flight_flagged", self._flag_lost_in_flight),
            ("ambiguous_resolved", self._resolve_ambiguous_calls),
            ("stale_calls_resolved", self._resolve_stale_in_progress),
            ("leaked_agents_released", self._release_leaked_agents),
            ("stale_agents_offline", self._handle_stale_agents),
            ("wrap_up_released", self._release_wrap_up_agents),
        ]
        for key, step in steps:
            try:
                results[key] = await step(session, results["errors"])
            except Exception as exc:
                await session.rollback()
                logger.exception("reconcile_step_error", step=key)
                results["errors"].append(f"{key}:{type(exc).__name__}:{exc}")

        logger.info("reconciliation_complete", **results)
        return results

    async def _repair(self, session: "AsyncSession", errors: list, op: str, key: str, work) -> object:
        """One repair in its own transaction; a failure is logged and recorded,
        and does not stop the remaining repairs."""
        try:
            return await run_transaction(session, work, op=f"reconcile:{op}")
        except Exception as exc:
            logger.exception("reconcile_item_error", step=op, item=key)
            errors.append(f"{op}:{key}:{type(exc).__name__}:{exc}")
            return None

    @staticmethod
    async def _ids(session: "AsyncSession", rows) -> list:
        """End the read transaction used to find candidates (no locks held)."""
        await session.commit()
        return rows

    # ── 1. Stale reservations ─────────────────────────────────────────────
    async def _resolve_stale_reserved(self, session: "AsyncSession", errors: list) -> int:
        cutoff = utcnow() - timedelta(seconds=_settings.worker_lease_seconds * 2)
        ids = await self._ids(session, [c.id for c in await CallRepository(session).list_stale_reserved(cutoff)])

        async def repair(s: "AsyncSession", call_id: str) -> str | None:
            call_repo, dispatch_repo = CallRepository(s), DispatchRepository(s)
            call = await call_repo.get_for_update(call_id)
            if call is None or call.state != CallState.RESERVED:
                return None
            dispatch = await dispatch_repo.get_by_call_id_for_update(call_id)
            if dispatch is None or dispatch.status == DispatchStatus.PENDING:
                # The provider was never contacted — safe to fail and release.
                reason = "NO_DISPATCH_RECORD" if dispatch is None else "DISPATCH_NEVER_CLAIMED"
                if not await finish_call(s, call, to_state=CallState.FAILED, reason=f"RECONCILE:{reason}"):
                    return None
                if dispatch is not None:
                    await dispatch_repo.update_result(
                        dispatch.id, status=DispatchStatus.FAILED, result_code=f"RECONCILE:{reason}"
                    )
                return reason
            if (
                dispatch.status in (DispatchStatus.CLAIMED, DispatchStatus.AMBIGUOUS)
                and dispatch.provider_call_id is None
                and not call.is_ambiguous
            ):
                # Conservative: flag for investigation. The call is still
                # RESERVED, so the dispatch worker may safely re-claim it.
                await self._mark_ambiguous(s, call_id)
                return "AMBIGUOUS"
            return None

        resolved = 0
        for call_id in ids:
            outcome = await self._repair(session, errors, "stale_reserved", call_id, lambda s: repair(s, call_id))
            if outcome == "AMBIGUOUS":
                reconciliation_actions.labels(action_type="stale_reserved_ambiguous").inc()
            elif outcome:
                resolved += 1
                reconciliation_actions.labels(action_type=f"stale_reserved_{outcome.lower()}").inc()
        return resolved

    # ── 2. Worker died while the request was with the provider ────────────
    async def _flag_lost_in_flight(self, session: "AsyncSession", errors: list) -> int:
        cutoff = utcnow()
        found = await DispatchRepository(session).list_lost_in_flight(cutoff)
        ids = await self._ids(session, [d.call_id for d in found if d.provider_call_id is None])

        async def repair(s: "AsyncSession", call_id: str) -> bool:
            if await CallRepository(s).get_for_update(call_id) is None:
                return False
            dispatch_repo = DispatchRepository(s)
            dispatch = await dispatch_repo.get_by_call_id_for_update(call_id)
            if (
                dispatch is None
                or dispatch.status != DispatchStatus.SENT
                or dispatch.provider_call_id is not None
                or as_utc(dispatch.lease_expires_at) >= cutoff
            ):
                return False  # the worker recorded an outcome meanwhile
            await dispatch_repo.update_result(
                dispatch.id, status=DispatchStatus.AMBIGUOUS, result_code="LEASE_EXPIRED_IN_FLIGHT"
            )
            await self._mark_ambiguous(s, call_id)
            return True

        flagged = 0
        for call_id in ids:
            if await self._repair(session, errors, "lost_in_flight", call_id, lambda s: repair(s, call_id)):
                flagged += 1
                reconciliation_actions.labels(action_type="lost_in_flight_ambiguous").inc()
        return flagged

    # ── 3. Ambiguous outcomes ─────────────────────────────────────────────
    async def _resolve_ambiguous_calls(self, session: "AsyncSession", errors: list) -> int:
        grace_cutoff = utcnow() - self._ambiguous_grace
        candidates = await self._ids(session, [
            (c.id, c.state, c.provider_call_id, c.provider_id, c.idempotency_key, c.updated_at)
            for c in await CallRepository(session).list_ambiguous()
        ])

        async def clear(s: "AsyncSession", call_id: str) -> bool:
            await self._clear_ambiguous(s, call_id)
            return True

        async def settle(s: "AsyncSession", call_id: str, provider_call_id: str | None) -> bool:
            call_repo, dispatch_repo = CallRepository(s), DispatchRepository(s)
            call = await call_repo.get_for_update(call_id)
            if call is None or call.provider_call_id is not None or call.state in TERMINAL_CALL_STATES:
                return False
            dispatch = await dispatch_repo.get_by_call_id_for_update(call_id)
            if provider_call_id is None:
                # Provider has no call for this idempotency key: it was never placed.
                if not await finish_call(s, call, to_state=CallState.FAILED, reason="RECONCILE_NOT_PLACED"):
                    return False
                if dispatch is not None:
                    await dispatch_repo.update_result(
                        dispatch.id, status=DispatchStatus.FAILED, result_code="RECONCILE_NOT_PLACED"
                    )
                reconciliation_actions.labels(action_type="ambiguous_not_placed").inc()
                return True
            await s.execute(
                update(Call)
                .where(Call.id == call_id)
                .values(provider_call_id=provider_call_id, is_ambiguous=False, updated_at=utcnow())
            )
            if dispatch is not None:
                await dispatch_repo.update_result(
                    dispatch.id,
                    status=DispatchStatus.SUCCEEDED,
                    provider_call_id=provider_call_id,
                    result_code="RECONCILE_ACCEPTED",
                )
            reconciliation_actions.labels(action_type="ambiguous_accepted").inc()
            return True

        resolved = 0
        for call_id, state, known_provider_call_id, provider_id, idempotency_key, updated_at in candidates:
            if state in TERMINAL_CALL_STATES:
                await self._repair(session, errors, "ambiguous_clear", call_id, lambda s: clear(s, call_id))
                continue
            if state == CallState.RESERVED:
                continue  # never sent; the dispatch worker will (re)dispatch it
            if known_provider_call_id is not None:
                # Outcome known (provider id recorded) — events drive it from here.
                if await self._repair(session, errors, "ambiguous_clear", call_id, lambda s: clear(s, call_id)):
                    resolved += 1
                continue
            if as_utc(updated_at) > grace_cutoff:
                continue

            provider = self._providers.get(provider_id or "")
            if provider is None:
                logger.warning("reconcile_no_provider_adapter", call_id=call_id, provider=provider_id)
                continue
            # Provider lookup with no transaction open (no row locks held).
            try:
                provider_call_id = await provider.find_call_by_idempotency_key(idempotency_key)
            except NotImplementedError:
                continue
            except Exception:
                logger.exception("reconcile_provider_lookup_failed", call_id=call_id)
                continue
            if await self._repair(
                session, errors, "ambiguous", call_id, lambda s: settle(s, call_id, provider_call_id)
            ):
                resolved += 1
        return resolved

    # ── 4. Calls whose outcome never arrived ──────────────────────────────
    async def _resolve_stale_in_progress(self, session: "AsyncSession", errors: list) -> int:
        cutoff = utcnow() - timedelta(seconds=_settings.stale_call_seconds)
        candidates = await self._ids(session, [
            (c.id, c.state, c.provider_id, c.provider_call_id)
            for c in await CallRepository(session).list_stale_in_progress(cutoff)
        ])

        async def finish(s: "AsyncSession", call_id: str, seen_state: str) -> bool:
            call = await CallRepository(s).get_for_update(call_id)
            if call is None or call.state != seen_state:
                return False
            contacted = call_was_contacted(call)
            to_state = CallState.COMPLETED if call.connected_at is not None else CallState.FAILED
            return await finish_call(
                s, call, to_state=to_state, reason="RECONCILE_OUTCOME_LOST", contacted=contacted
            )

        resolved = 0
        for call_id, state, provider_id, provider_call_id in candidates:
            provider = self._providers.get(provider_id or "")
            if provider is None or provider_call_id is None:
                continue
            try:
                status = await provider.get_call_status(provider_call_id)
            except Exception:
                logger.exception("reconcile_probe_failed", call_id=call_id)
                continue
            if status is not None:
                continue  # provider says the call is still active
            if await self._repair(session, errors, "stale_call", call_id, lambda s: finish(s, call_id, state)):
                resolved += 1
                reconciliation_actions.labels(action_type="stale_call_finished").inc()
        return resolved

    # ── 5. Leaked agents ──────────────────────────────────────────────────
    async def _release_leaked_agents(self, session: "AsyncSession", errors: list) -> int:
        result = await session.execute(
            select(Agent.id, Agent.state, Agent.current_call_id, Call.state)
            .outerjoin(Call, Agent.current_call_id == Call.id)
            .where(Agent.state.in_([AgentState.RESERVED, AgentState.DIALING, AgentState.CONNECTED]))
        )
        candidates = await self._ids(session, [
            (agent_id, state, call_id)
            for agent_id, state, call_id, call_state in result.all()
            if call_state is None or call_state in TERMINAL_CALL_STATES
        ])

        async def release(s: "AsyncSession", agent_id: str, seen_state: str, seen_call_id: str | None) -> bool:
            # Agent-only repair: the call is read, not locked (a finished call
            # never changes again; a missing one never appears under this id).
            locked = await AgentRepository(s).get_for_update(agent_id)
            if locked is None or locked.current_call_id != seen_call_id or locked.state != seen_state:
                return False
            if seen_call_id is not None:
                call = await CallRepository(s).get(seen_call_id)
                if call is not None and call.state not in TERMINAL_CALL_STATES:
                    return False
            if not await release_orphaned_agent(s, locked):
                return False
            logger.warning("leaked_agent_released", agent_id=agent_id, from_state=seen_state, call_id=seen_call_id)
            return True

        released = 0
        for agent_id, state, call_id in candidates:
            if await self._repair(
                session, errors, "leaked_agent", agent_id, lambda s: release(s, agent_id, state, call_id)
            ):
                released += 1
                reconciliation_actions.labels(action_type="leaked_agent_released").inc()
        return released

    # ── 6. Stale heartbeats ───────────────────────────────────────────────
    async def _handle_stale_agents(self, session: "AsyncSession", errors: list) -> int:
        cutoff = utcnow() - timedelta(seconds=_settings.agent_heartbeat_timeout_seconds)
        candidates = await self._ids(session, [
            a.id for a in await AgentRepository(session).list_stale_heartbeats(cutoff)
        ])

        async def offline(s: "AsyncSession", agent_id: str) -> bool:
            agent = await AgentRepository(s).get_for_update(agent_id)
            # Same states list_stale_heartbeats() selects.
            if agent is None or agent.state not in (
                AgentState.AVAILABLE, AgentState.RESERVED, AgentState.DIALING, AgentState.CONNECTED,
            ):
                return False
            if agent.last_heartbeat is None or as_utc(agent.last_heartbeat) >= cutoff:
                return False  # heartbeat arrived meanwhile
            if agent.current_call_id:
                call = await CallRepository(s).get(agent.current_call_id)
                if call and call.state not in TERMINAL_CALL_STATES:
                    # Agent has an active call — cannot safely mark offline
                    logger.warning(
                        "stale_agent_has_active_call",
                        agent_id=agent_id,
                        call_id=agent.current_call_id,
                        call_state=call.state,
                    )
                    reconciliation_actions.labels(action_type="stale_agent_preserved").inc()
                    return False
            # Use conditional update to avoid overwriting a newer state
            return await AgentRepository(s).transition_state(
                agent_id,
                from_state=agent.state,
                to_state=AgentState.OFFLINE,
                version=agent.version,
                clear_call=True,
            )

        resolved = 0
        for agent_id in candidates:
            if await self._repair(session, errors, "stale_agent", agent_id, lambda s: offline(s, agent_id)):
                resolved += 1
                reconciliation_actions.labels(action_type="stale_agent_offlined").inc()
                logger.info("stale_agent_offlined", agent_id=agent_id)
        return resolved

    # ── 7. Wrap-up timeout ────────────────────────────────────────────────
    async def _release_wrap_up_agents(self, session: "AsyncSession", errors: list) -> int:
        result = await session.execute(
            select(Agent.id, Agent.version, Agent.state_changed_at, Campaign.wrap_up_seconds)
            .join(Campaign, Agent.campaign_id == Campaign.id)
            .where(Agent.state == AgentState.WRAP_UP)
        )
        now = utcnow()
        due = await self._ids(session, [
            (agent_id, version)
            for agent_id, version, changed_at, wrap_up_seconds in result.all()
            if now >= as_utc(changed_at) + timedelta(seconds=wrap_up_seconds)
        ])

        async def release(s: "AsyncSession", agent_id: str, version: int) -> bool:
            # Conditional on the version seen: an agent that moved on is untouched.
            return await AgentRepository(s).transition_state(
                agent_id, from_state=AgentState.WRAP_UP, to_state=AgentState.AVAILABLE, version=version
            )

        released = 0
        for agent_id, version in due:
            if await self._repair(session, errors, "wrap_up", agent_id, lambda s: release(s, agent_id, version)):
                released += 1
                reconciliation_actions.labels(action_type="wrap_up_released").inc()
        return released

    async def _mark_ambiguous(self, session, call_id: str) -> None:
        await session.execute(
            update(Call).where(Call.id == call_id).values(is_ambiguous=True, updated_at=utcnow())
        )

    async def _clear_ambiguous(self, session, call_id: str) -> None:
        await session.execute(update(Call).where(Call.id == call_id).values(is_ambiguous=False))
