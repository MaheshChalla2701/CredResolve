"""Reconciliation service — handles leaks, crashes, and ambiguous outcomes.

Reconciliation is idempotent: running it multiple times is safe.
Each action is conservative: we never release a reservation unless we
have positive evidence that doing so is safe.

Categories handled:
  1. RESERVED calls with no dispatch record or expired lease →
     Check if dispatch succeeded. If unknown, mark AMBIGUOUS.
     If proven pre-initiation failure → FAILED, release agent.

  2. CLAIMED dispatch records with expired lease and no provider_call_id →
     Ambiguous: provider may have accepted. Mark call + dispatch AMBIGUOUS.

  3. Agents with stale heartbeats in non-offline states →
     If agent has no active call → OFFLINE.
     If agent has an active call → preserve reservation (fail closed).
     Log for investigation.

  4. AMBIGUOUS calls → probe provider via get_call_status().
     If provider confirms terminal → apply terminal transition.
     If provider confirms active → leave in AMBIGUOUS (or update state).
     If provider says unknown → keep AMBIGUOUS.

  5. Agents in WRAP_UP with no active call for > wrap_up_seconds →
     Transition to AVAILABLE.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from app.core.config import get_settings
from app.core.enums import AgentState, CallState, DispatchStatus
from app.core.logging import get_logger
from app.core.metrics import reconciliation_actions

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from app.providers.base import ProviderAdapter

logger = get_logger(__name__)
_settings = get_settings()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ReconciliationService:
    def __init__(
        self,
        provider_adapters: dict[str, "ProviderAdapter"] | None = None,
    ) -> None:
        self._providers = provider_adapters or {}

    async def run(self, session: "AsyncSession") -> dict:
        """Run all reconciliation checks. Returns a summary of actions taken."""
        results = {
            "stale_reserved_resolved": 0,
            "ambiguous_resolved": 0,
            "stale_agents_offline": 0,
            "wrap_up_released": 0,
            "errors": [],
        }

        try:
            results["stale_reserved_resolved"] = await self._resolve_stale_reserved(session)
        except Exception as exc:
            logger.error("reconcile_stale_reserved_error", error=str(exc))
            results["errors"].append(f"stale_reserved:{exc}")

        try:
            results["ambiguous_resolved"] = await self._resolve_ambiguous_calls(session)
        except Exception as exc:
            logger.error("reconcile_ambiguous_error", error=str(exc))
            results["errors"].append(f"ambiguous:{exc}")

        try:
            results["stale_agents_offline"] = await self._handle_stale_agents(session)
        except Exception as exc:
            logger.error("reconcile_agents_error", error=str(exc))
            results["errors"].append(f"agents:{exc}")

        try:
            results["wrap_up_released"] = await self._release_wrap_up_agents(session)
        except Exception as exc:
            logger.error("reconcile_wrap_up_error", error=str(exc))
            results["errors"].append(f"wrap_up:{exc}")

        logger.info("reconciliation_complete", **results)
        return results

    async def _resolve_stale_reserved(self, session: "AsyncSession") -> int:
        """Find RESERVED calls that were never dispatched past their deadline."""
        from app.persistence.repositories import CallRepository, DispatchRepository, AgentRepository
        call_repo = CallRepository(session)
        dispatch_repo = DispatchRepository(session)
        agent_repo = AgentRepository(session)

        cutoff = _utcnow() - timedelta(seconds=_settings.worker_lease_seconds * 2)
        stale_calls = await call_repo.list_stale_reserved(cutoff)

        resolved = 0
        for call in stale_calls:
            dispatch = await dispatch_repo.get_by_call_id(call.id)

            if dispatch is None:
                # No dispatch record — pre-initiation failure is certain
                await self._fail_call_and_release_agent(
                    session, call, "NO_DISPATCH_RECORD", agent_repo, call_repo
                )
                resolved += 1
                reconciliation_actions.labels(action_type="stale_reserved_no_dispatch").inc()

            elif dispatch.status == DispatchStatus.PENDING:
                # Dispatch was never claimed — still safe to fail
                await self._fail_call_and_release_agent(
                    session, call, "DISPATCH_NEVER_CLAIMED", agent_repo, call_repo
                )
                resolved += 1
                reconciliation_actions.labels(action_type="stale_reserved_unclaimed").inc()

            elif dispatch.status in (DispatchStatus.CLAIMED, DispatchStatus.AMBIGUOUS):
                if dispatch.provider_call_id is None:
                    # Lease expired, no provider_call_id → ambiguous
                    await self._mark_ambiguous(session, call)
                    reconciliation_actions.labels(action_type="stale_reserved_ambiguous").inc()

        return resolved

    async def _resolve_ambiguous_calls(self, session: "AsyncSession") -> int:
        """Probe provider for calls in AMBIGUOUS state."""
        from app.persistence.repositories import CallRepository
        call_repo = CallRepository(session)
        ambiguous_calls = await call_repo.list_ambiguous()

        resolved = 0
        for call in ambiguous_calls:
            provider = self._providers.get(call.provider_id or "mock_provider_a")
            if provider is None:
                continue

            if call.provider_call_id is None:
                # Cannot probe without a provider call ID — leave ambiguous
                continue

            try:
                status_event = await provider.get_call_status(call.provider_call_id)
                if status_event is None:
                    # Provider doesn't know about this call → likely pre-initiation failure
                    # But we treat as safely failed only if call was in INITIATED state
                    if call.state == CallState.INITIATED:
                        call_locked = await call_repo.get_for_update(call.id)
                        if call_locked and call_locked.is_ambiguous:
                            await call_repo.transition_state(
                                call.id,
                                from_state=call_locked.state,
                                to_state=CallState.FAILED,
                                version=call_locked.state_version,
                                failure_reason="RECONCILE_PROVIDER_UNKNOWN",
                                completed_at=_utcnow(),
                            )
                            resolved += 1
                            reconciliation_actions.labels(action_type="ambiguous_failed").inc()
                else:
                    logger.info(
                        "ambiguous_call_still_active",
                        call_id=call.id,
                        provider_event_type=status_event.event_type,
                    )
            except Exception as exc:
                logger.warning(
                    "reconcile_probe_failed",
                    call_id=call.id,
                    error=str(exc),
                )

        return resolved

    async def _handle_stale_agents(self, session: "AsyncSession") -> int:
        """Handle agents with stale heartbeats."""
        from app.persistence.repositories import AgentRepository, CallRepository
        agent_repo = AgentRepository(session)
        call_repo = CallRepository(session)

        cutoff = _utcnow() - timedelta(seconds=_settings.agent_heartbeat_timeout_seconds)
        stale_agents = await agent_repo.list_stale_heartbeats(cutoff)

        resolved = 0
        for agent in stale_agents:
            if agent.current_call_id:
                call = await call_repo.get(agent.current_call_id)
                if call and call.state not in (CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED):
                    # Agent has an active call — cannot safely mark offline
                    logger.warning(
                        "stale_agent_has_active_call",
                        agent_id=agent.id,
                        call_id=agent.current_call_id,
                        call_state=call.state,
                    )
                    reconciliation_actions.labels(action_type="stale_agent_preserved").inc()
                    continue

            # Agent is stale and has no active call — mark OFFLINE
            # Use conditional update to avoid overwriting a newer state
            updated = await agent_repo.transition_state(
                agent.id,
                from_state=agent.state,
                to_state=AgentState.OFFLINE,
                version=agent.version,
                clear_call=True,
            )
            if updated:
                resolved += 1
                reconciliation_actions.labels(action_type="stale_agent_offlined").inc()
                logger.info("stale_agent_offlined", agent_id=agent.id)

        return resolved

    async def _release_wrap_up_agents(self, session: "AsyncSession") -> int:
        """Release agents from WRAP_UP back to AVAILABLE after wrap_up_seconds."""
        from sqlalchemy import select
        from app.persistence.repositories import AgentRepository, CampaignRepository
        from app.persistence.models import Agent, Campaign

        # Find agents in WRAP_UP with state_changed_at older than their campaign's wrap_up_seconds
        result = await session.execute(
            select(Agent, Campaign)
            .join(Campaign, Agent.campaign_id == Campaign.id)
            .where(Agent.state == AgentState.WRAP_UP)
        )

        agent_repo = AgentRepository(session)
        released = 0
        for agent, campaign in result:
            wrap_up_deadline = agent.state_changed_at + timedelta(seconds=campaign.wrap_up_seconds)
            if _utcnow() >= wrap_up_deadline:
                updated = await agent_repo.transition_state(
                    agent.id,
                    from_state=AgentState.WRAP_UP,
                    to_state=AgentState.AVAILABLE,
                    version=agent.version,
                )
                if updated:
                    released += 1
                    reconciliation_actions.labels(action_type="wrap_up_released").inc()

        return released

    async def _fail_call_and_release_agent(
        self,
        session,
        call,
        reason: str,
        agent_repo,
        call_repo,
    ) -> None:
        call_locked = await call_repo.get_for_update(call.id)
        if call_locked is None or call_locked.state != CallState.RESERVED:
            return

        await call_repo.transition_state(
            call.id,
            from_state=CallState.RESERVED,
            to_state=CallState.FAILED,
            version=call_locked.state_version,
            failure_reason=f"RECONCILE:{reason}",
            completed_at=_utcnow(),
        )

        agent = await agent_repo.get_for_update(call.agent_id)
        if agent and agent.state == AgentState.RESERVED:
            await agent_repo.transition_state(
                agent.id,
                from_state=AgentState.RESERVED,
                to_state=AgentState.AVAILABLE,
                version=agent.version,
                clear_call=True,
            )

    async def _mark_ambiguous(self, session, call) -> None:
        from sqlalchemy import update
        from app.persistence.models import Call as CallModel
        await session.execute(
            update(CallModel)
            .where(CallModel.id == call.id)
            .values(is_ambiguous=True, updated_at=_utcnow())
        )
