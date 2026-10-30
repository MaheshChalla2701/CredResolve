"""PostgreSQL-only concurrency checks — skipped unless a throwaway PostgreSQL
database is supplied. The rest of the suite runs on SQLite, which ignores
FOR UPDATE / SKIP LOCKED and has a single writer, so it cannot prove these.

    docker compose up -d postgres_test        # tmpfs-backed, port 5433
    export SMARTDIALER_TEST_POSTGRES_URL=postgresql+asyncpg://smartdialer_test:smartdialer_test@localhost:5433/smartdialer_test
    DATABASE_URL=postgresql+psycopg://smartdialer_test:smartdialer_test@localhost:5433/smartdialer_test alembic upgrade head
    pytest tests/postgres -v

The tests only insert their own uniquely named rows; they never create, drop or
truncate tables. The schema must already be at the Alembic head revision.
"""
from __future__ import annotations

import asyncio
import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import func, inspect, select, text

from app.core.enums import ACTIVE_CALL_STATES, AgentState, DispatchStatus
from app.core.errors import AllocationError
from app.persistence.models import Agent, Base, Borrower, Call, Campaign, DispatchRecord

ASYNC_URL = os.environ.get("SMARTDIALER_TEST_POSTGRES_URL", "")

pytestmark = [
    pytest.mark.concurrency,
    pytest.mark.skipif(
        not ASYNC_URL.startswith("postgresql"),
        reason="set SMARTDIALER_TEST_POSTGRES_URL to a throwaway PostgreSQL database to run",
    ),
]


@pytest_asyncio.fixture
async def pg():
    """Point the application's session factory at PostgreSQL for one test."""
    from app.persistence import database

    sync_url = ASYNC_URL.replace("+asyncpg", "+psycopg")
    database.override_engines(sync_url, ASYNC_URL)
    try:
        yield database.AsyncSessionLocal
    finally:
        await database.async_engine.dispose()
        database.sync_engine.dispose()


async def _campaign(factory, *, cap: int, agents: int, borrowers: int, mode: str = "progressive") -> str:
    cid = str(uuid.uuid4())
    async with factory() as s:
        s.add(Campaign(id=cid, name=f"pg-{cid[:8]}", status="ACTIVE", dialing_mode=mode, max_concurrent_calls=cap))
        await s.flush()
        for i in range(agents):
            s.add(Agent(campaign_id=cid, name=f"A{i}", state=AgentState.AVAILABLE))
        for i in range(borrowers):
            s.add(Borrower(campaign_id=cid, synthetic_name=f"B{i}", masked_phone=f"***-{i}", status="QUEUED"))
        await s.commit()
    return cid


async def _active(factory, cid: str) -> int:
    async with factory() as s:
        return (await s.execute(select(func.count(Call.id)).where(
            Call.campaign_id == cid, Call.state.in_(list(ACTIVE_CALL_STATES))))).scalar_one()


@pytest.mark.asyncio
async def test_schema_is_at_alembic_head_and_matches_models(pg) -> None:
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    from app.persistence import database

    head = ScriptDirectory.from_config(Config("alembic.ini")).get_current_head()
    async with pg() as s:
        current = (await s.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
    assert current == head

    inspector = inspect(database.sync_engine)
    for table in Base.metadata.sorted_tables:
        db_columns = {c["name"] for c in inspector.get_columns(table.name)}
        assert db_columns == {c.name for c in table.columns}, table.name


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["progressive", "predictive"])
async def test_parallel_dialing_cycles_respect_campaign_cap(pg, mode: str) -> None:
    from app.services.dialing import run_dialing_cycle
    from app.services.safety_controller import SafetyController

    cid = await _campaign(pg, cap=4, agents=12, borrowers=40, mode=mode)
    safety = SafetyController()

    async def cycle() -> int:
        async with pg() as s:
            result = await run_dialing_cycle(s, await s.get(Campaign, cid), provider_id="mock_provider_a", safety=safety)
            await s.commit()
            return len(result.calls)

    allocated = await asyncio.gather(*[cycle() for _ in range(10)])
    assert sum(allocated) == await _active(pg, cid) <= 4


@pytest.mark.asyncio
async def test_parallel_allocations_never_double_book(pg) -> None:
    from app.services.call_allocator import CallAllocator

    cid = await _campaign(pg, cap=100, agents=8, borrowers=30)
    allocator = CallAllocator()

    async def attempt() -> None:
        async with pg() as s:
            try:
                await allocator.allocate_one(s, cid)
                await s.commit()
            except AllocationError:
                await s.rollback()

    await asyncio.gather(*[attempt() for _ in range(20)])
    async with pg() as s:
        calls = (await s.execute(select(Call).where(Call.campaign_id == cid))).scalars().all()
    assert len(calls) == 8
    assert len({c.agent_id for c in calls}) == 8
    assert len({c.borrower_id for c in calls}) == 8


@pytest.mark.asyncio
async def test_parallel_dispatch_claims_are_disjoint(pg) -> None:
    """Workers claiming at the same time (FOR UPDATE SKIP LOCKED + conditional
    UPDATE) must never claim the same outbox record twice."""
    from app.persistence.repositories import DispatchRepository
    from app.services.call_allocator import CallAllocator

    cid = await _campaign(pg, cap=100, agents=12, borrowers=12)
    provider_id = f"pg-claim-{cid[:8]}"
    async with pg() as s:
        await CallAllocator(provider_id=provider_id).allocate_batch(s, cid, 12)
        await s.commit()

    async def claim(worker: str) -> list[str]:
        async with pg() as s:
            records = await DispatchRepository(s).claim_pending(worker, batch_size=5, provider_id=provider_id)
            await asyncio.sleep(0.05)  # hold the row locks while the others claim
            await s.commit()
            return [r.id for r in records]

    results = await asyncio.gather(*[claim(f"w{i}") for i in range(4)])
    claimed = [rid for ids in results for rid in ids]
    assert len(claimed) == len(set(claimed)) == 12
    async with pg() as s:
        statuses = (await s.execute(select(DispatchRecord.status).where(
            DispatchRecord.provider_id == provider_id))).scalars().all()
    assert set(statuses) == {DispatchStatus.CLAIMED}


@pytest.mark.asyncio
async def test_parallel_first_use_of_provider_health(pg) -> None:
    from app.services.provider_health import ProviderHealthService

    health = ProviderHealthService(f"pg-health-{uuid.uuid4().hex[:8]}")

    async def fail_once() -> None:
        async with pg() as s:
            await health.record_failure(s)
            await s.commit()

    await asyncio.gather(*[fail_once() for _ in range(6)])
    async with pg() as s:
        snapshot = await health.get_snapshot(s)
    assert snapshot["consecutive_failures"] == 6
    assert snapshot["circuit_state"] == "OPEN"


@pytest.mark.asyncio
async def test_real_deadlock_is_detected_and_retried(pg) -> None:
    """Two transactions lock the same two rows in opposite order. PostgreSQL
    aborts one with SQLSTATE 40P01; run_transaction must classify that as
    transient and run the victim again, so both units of work complete."""
    from sqlalchemy import update
    from app.persistence.retry import run_transaction
    from scenarios import availability_campaign

    _, (first, second) = await availability_campaign(pg, agents=2, borrowers=0)
    attempts = {"t1": 0, "t2": 0}
    both_hold_one_lock = asyncio.Barrier(2)

    async def tx(name: str, a: str, b: str) -> None:
        async def work(s):
            attempts[name] += 1
            await s.execute(update(Agent).where(Agent.id == a).values(name=f"{name}-first"))
            if attempts[name] == 1:
                await both_hold_one_lock.wait()
            await s.execute(update(Agent).where(Agent.id == b).values(name=f"{name}-second"))

        async with pg() as s:
            await run_transaction(s, work, op=name, base_delay=0.01)

    await asyncio.wait_for(asyncio.gather(tx("t1", first, second), tx("t2", second, first)), timeout=30)
    assert sorted(attempts.values()) == [1, 2]  # exactly one deadlock victim, retried once


@pytest.mark.asyncio
async def test_reconciliation_and_events_race_in_parallel(pg) -> None:
    from scenarios import reconciliation_vs_events

    outcome = await reconciliation_vs_events(pg, calls=40, reconcilers=3)
    assert outcome["calls"] == 40
    assert outcome["errors"] == []
    assert outcome["inconsistent"] == []


@pytest.mark.asyncio
async def test_agent_paused_by_another_transaction_after_approval(pg) -> None:
    """A real concurrent pause: committed by a separate transaction while the
    dialing cycle holds the campaign lock and has already been approved."""
    from app.core.enums import BorrowerStatus
    from scenarios import PausingSafetyController, availability_campaign, availability_state, dial, pause_agent

    cid, ids = await availability_campaign(pg, agents=3, borrowers=5)

    async def other_transaction_pauses(_dial_session) -> None:
        async with pg() as other:
            assert await pause_agent(other, ids[0])
            await other.commit()

    result = await dial(pg, cid, PausingSafetyController(other_transaction_pauses))
    assert (result.decision.approved_count, len(result.calls)) == (3, 2)
    state = await availability_state(pg, cid)
    _, approved, allocated, reasons = state["record"]
    assert (approved, allocated) == (3, 2)
    assert "ALLOCATION_SHORTFALL:NO_AVAILABLE_AGENT" in reasons
    assert state["agents"][ids[0]] == (AgentState.PAUSED, None)
    assert state["reserved_agents_with_their_call"] == 2
    assert state["calls"] == state["pending_dispatches"] == 2
    assert state["borrowers"] == sorted([(BorrowerStatus.ALLOCATED, 1)] * 2 + [(BorrowerStatus.QUEUED, 0)] * 3)
