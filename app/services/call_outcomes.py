"""Call outcome rules shared by the event processor, dispatch worker and reconciliation.

This is the single place that decides what happens to the agent and the
borrower when a call moves forward or ends, so every path applies the same rules:

  Agent (only touched while it is still assigned to this call):
    * call ringing / answered        -> DIALING
    * call connected                 -> CONNECTED
    * call ended, borrower answered  -> WRAP_UP (released to AVAILABLE by
                                        reconciliation after wrap_up_seconds)
    * call ended, never answered     -> AVAILABLE immediately

  Borrower (settled exactly once, only while still ALLOCATED):
    * borrower was contacted         -> COMPLETED
    * not contacted, attempts left   -> QUEUED with next_eligible_at = now + cooldown
    * not contacted, attempts used   -> EXHAUSTED

All agent moves go through VALID_AGENT_TRANSITIONS; multi-step moves (e.g.
RESERVED -> DIALING -> WRAP_UP when the first event we see is terminal) apply
each step as its own validated, version-checked update.
"""
from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.core.enums import (
    TERMINAL_CALL_STATES,
    VALID_AGENT_TRANSITIONS,
    AgentState,
    BorrowerStatus,
    CallState,
)
from app.core.errors import StaleUpdateError
from app.core.logging import get_logger
from app.core.timeutils import utcnow
from app.domain.state_machines import validate_call_transition
from app.persistence.models import Agent, Borrower, Call
from app.persistence.repositories import (
    AgentRepository,
    BorrowerRepository,
    CallRepository,
    CampaignRepository,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


def call_was_contacted(call: Call, *, final_state: str | None = None) -> bool:
    """True if the borrower answered. A COMPLETED call always implies contact."""
    return (
        call.answered_at is not None
        or call.connected_at is not None
        or (final_state or call.state) == CallState.COMPLETED
    )


def _agent_path(from_state: str, to_state: str) -> list[str] | None:
    """Valid transition path from -> to (direct, or via DIALING). None if impossible."""
    if from_state == to_state:
        return []
    if (AgentState(from_state), AgentState(to_state)) in VALID_AGENT_TRANSITIONS:
        return [to_state]
    if (
        (AgentState(from_state), AgentState.DIALING) in VALID_AGENT_TRANSITIONS
        and (AgentState.DIALING, AgentState(to_state)) in VALID_AGENT_TRANSITIONS
    ):
        return [AgentState.DIALING, to_state]
    return None


async def _move_agent(
    session: "AsyncSession", agent: Agent, target: str, *, clear_call: bool
) -> str | None:
    path = _agent_path(agent.state, target)
    if path is None:
        logger.warning(
            "agent_transition_not_allowed",
            agent_id=agent.id,
            from_state=agent.state,
            to_state=target,
        )
        return None

    repo = AgentRepository(session)
    state, version = agent.state, agent.version
    for i, step in enumerate(path):
        last = i == len(path) - 1
        updated = await repo.transition_state(
            agent.id,
            from_state=state,
            to_state=step,
            version=version,
            clear_call=clear_call and last,
        )
        if not updated:
            raise StaleUpdateError(f"Agent {agent.id} changed concurrently ({state} -> {step})")
        state, version = step, version + 1
    return state


async def move_agent_for_call(
    session: "AsyncSession", call: Call, target: str, *, clear_call: bool = False
) -> str | None:
    """Move the call's agent to target, if the agent is still assigned to this call."""
    agent = await AgentRepository(session).get_for_update(call.agent_id)
    if agent is None:
        logger.error("call_agent_missing", call_id=call.id, agent_id=call.agent_id)
        return None
    if agent.current_call_id != call.id:
        # The agent was already released (e.g. by reconciliation) and may be on
        # another call now — a late event for this call must not touch it.
        logger.info(
            "agent_no_longer_on_call",
            call_id=call.id,
            agent_id=agent.id,
            agent_call_id=agent.current_call_id,
        )
        return None
    return await _move_agent(session, agent, target, clear_call=clear_call)


async def release_agent_after_call(
    session: "AsyncSession", call: Call, *, contacted: bool
) -> str | None:
    """Free the agent of a call that just ended (WRAP_UP if answered, else AVAILABLE)."""
    agent = await AgentRepository(session).get_for_update(call.agent_id)
    if agent is None:
        logger.error("call_agent_missing", call_id=call.id, agent_id=call.agent_id)
        return None
    if agent.current_call_id != call.id:
        logger.info("agent_no_longer_on_call", call_id=call.id, agent_id=agent.id)
        return None
    # An agent that was talking always wraps up, whatever the provider reported.
    target = (
        AgentState.WRAP_UP
        if contacted or agent.state == AgentState.CONNECTED
        else AgentState.AVAILABLE
    )
    return await _move_agent(session, agent, target, clear_call=True)


async def release_orphaned_agent(session: "AsyncSession", agent: Agent) -> str | None:
    """Release an agent whose current call is finished or missing (leak recovery)."""
    target = AgentState.WRAP_UP if agent.state == AgentState.CONNECTED else AgentState.AVAILABLE
    return await _move_agent(session, agent, target, clear_call=True)


async def settle_borrower(
    session: "AsyncSession", call: Call, *, contacted: bool
) -> str | None:
    """Apply the retry policy to the call's borrower. Returns the new status, or
    None if the borrower had already been settled."""
    result = await session.execute(
        select(Borrower)
        .where(Borrower.id == call.borrower_id)
        .execution_options(populate_existing=True)
    )
    borrower = result.scalar_one_or_none()
    if borrower is None:
        logger.error("call_borrower_missing", call_id=call.id, borrower_id=call.borrower_id)
        return None

    campaign = await CampaignRepository(session).get(call.campaign_id)
    next_eligible = None
    if contacted:
        status = BorrowerStatus.COMPLETED
    else:
        limit = borrower.max_attempts
        if campaign is not None:
            limit = min(limit, campaign.max_attempts_per_borrower)
        if borrower.attempt_count >= limit:
            status = BorrowerStatus.EXHAUSTED
        else:
            status = BorrowerStatus.QUEUED
            cooldown = campaign.retry_cooldown_seconds if campaign is not None else 0
            next_eligible = utcnow() + timedelta(seconds=cooldown)

    settled = await BorrowerRepository(session).settle_allocation(
        borrower.id, status, next_eligible_at=next_eligible
    )
    if not settled:
        logger.info("borrower_already_settled", borrower_id=borrower.id, call_id=call.id)
        return None
    logger.info(
        "borrower_settled",
        borrower_id=borrower.id,
        call_id=call.id,
        status=status,
        attempts=borrower.attempt_count,
    )
    return status


async def finish_call(
    session: "AsyncSession",
    call: Call,
    *,
    to_state: str,
    reason: str | None = None,
    contacted: bool = False,
) -> bool:
    """End a call that the caller has locked: terminal transition, then release the
    agent and settle the borrower. Returns False if the call had already moved on."""
    if call.state in TERMINAL_CALL_STATES:
        return False
    validate_call_transition(call.state, to_state)
    values: dict = {"completed_at": utcnow(), "is_ambiguous": False}
    if reason is not None:
        values["failure_reason"] = reason
    updated = await CallRepository(session).transition_state(
        call.id,
        from_state=call.state,
        to_state=to_state,
        version=call.state_version,
        **values,
    )
    if not updated:
        return False
    await release_agent_after_call(session, call, contacted=contacted)
    await settle_borrower(session, call, contacted=contacted)
    logger.info("call_finished", call_id=call.id, state=to_state, reason=reason)
    return True
