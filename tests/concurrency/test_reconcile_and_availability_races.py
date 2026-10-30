"""Races between reconciliation and provider events, and an agent changing
availability between the safety decision and allocation (SQLite; the same
scenarios run truly in parallel in tests/postgres)."""
from __future__ import annotations

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, BorrowerStatus
from app.persistence.database import AsyncSessionLocal
from app.providers.mock_provider_a import MockProviderA
from app.workers.runtime import DialerRuntime
from scenarios import (
    PausingSafetyController,
    availability_campaign,
    availability_state,
    dial,
    pause_agent,
    reconciliation_vs_events,
)


@pytest.mark.asyncio
async def test_reconciliation_and_events_finish_each_call_exactly_once(db_session: AsyncSession) -> None:
    outcome = await reconciliation_vs_events(AsyncSessionLocal, calls=20)
    assert outcome["calls"] == 20
    assert outcome["errors"] == []
    assert outcome["inconsistent"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("to_state", [AgentState.PAUSED, AgentState.OFFLINE])
async def test_agent_leaving_after_approval_is_recorded_as_shortfall(db_session: AsyncSession, to_state) -> None:
    cid, agent_ids = await availability_campaign(AsyncSessionLocal, agents=3, borrowers=5)

    async def agent_leaves(session) -> None:
        # Same transaction as the dial: on SQLite the dial holds the write lock,
        # so this is the only way the change can land inside the window.
        assert await pause_agent(session, agent_ids[0], to_state)

    result = await dial(AsyncSessionLocal, cid, PausingSafetyController(agent_leaves))
    assert result.decision.approved_count == 3
    assert len(result.calls) == 2

    state = await availability_state(AsyncSessionLocal, cid)
    decision, approved, allocated, reasons = state["record"]
    assert (approved, allocated) == (3, 2)
    assert "ALLOCATION_SHORTFALL:NO_AVAILABLE_AGENT" in reasons
    assert state["agents"][agent_ids[0]] == (to_state, None)  # no reservation for the agent that left
    assert state["reserved_agents_with_their_call"] == 2
    assert state["calls"] == state["pending_dispatches"] == 2
    assert state["borrowers"] == sorted([(BorrowerStatus.ALLOCATED, 1)] * 2 + [(BorrowerStatus.QUEUED, 0)] * 3)


@pytest.mark.asyncio
async def test_reserved_agent_cannot_be_paused_or_taken_offline(db_session: AsyncSession) -> None:
    from app.main import app

    cid, agent_ids = await availability_campaign(AsyncSessionLocal, agents=1, borrowers=1)
    app.state.runtime = DialerRuntime({"mock_provider_a": MockProviderA(seed=1, delay_scale=0.01)})
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test/api/v1") as c:
            assert (await c.post(f"/campaigns/{cid}/dial")).json()["calls_allocated"] == 1
            for target in ("PAUSED", "OFFLINE"):
                resp = await c.post(f"/agents/{agent_ids[0]}/availability", json={"state": target})
                assert resp.status_code == 400, resp.text
            agent = (await c.get(f"/agents/{agent_ids[0]}")).json()
    finally:
        await app.state.runtime.stop()
        app.state.runtime = None
    assert agent["state"] == "RESERVED"
    assert agent["current_call_id"] is not None
