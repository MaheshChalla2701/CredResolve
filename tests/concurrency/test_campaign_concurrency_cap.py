"""Regression tests: concurrent dialing must never exceed a campaign's
max_concurrent_calls.

Before the campaign lock, each concurrent dialing cycle read the same active
call count, got its own approval, and the cycles together reserved more calls
than the cap. These run on SQLite (as the whole suite does); the PostgreSQL
variant is tests/postgres/test_postgres_concurrency.py.
"""
from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import ACTIVE_CALL_STATES, AgentState
from app.core.errors import AllocationError
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Call, Campaign, SafetyDecisionRecord
from app.providers.mock_provider_a import MockProviderA
from app.services.call_allocator import CallAllocator
from app.services.dialing import run_dialing_cycle
from app.services.safety_controller import SafetyController
from app.workers.runtime import DialerRuntime


async def _campaign(*, mode: str, cap: int, agents: int, borrowers: int) -> str:
    cid = str(uuid.uuid4())
    async with AsyncSessionLocal() as s:
        s.add(Campaign(id=cid, name=f"cap-{mode}", status="ACTIVE", dialing_mode=mode,
                       max_concurrent_calls=cap))
        await s.flush()
        for i in range(agents):
            s.add(Agent(campaign_id=cid, name=f"A{i}", state=AgentState.AVAILABLE))
        for i in range(borrowers):
            s.add(Borrower(campaign_id=cid, synthetic_name=f"B{i}", masked_phone=f"***-{i}", status="QUEUED"))
        await s.commit()
    return cid


async def _active_calls(cid: str) -> int:
    async with AsyncSessionLocal() as s:
        return (await s.execute(
            select(func.count(Call.id)).where(Call.campaign_id == cid, Call.state.in_(list(ACTIVE_CALL_STATES)))
        )).scalar_one()


@pytest_asyncio.fixture
async def api_without_workers(db_session: AsyncSession):
    """The API with a runtime whose background loops are NOT started, so reserved
    calls stay active (RESERVED) and the cap is under maximum pressure."""
    from app.main import app

    app.state.runtime = DialerRuntime({"mock_provider_a": MockProviderA(seed=1, delay_scale=0.01)})
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test/api/v1") as client:
            yield client
    finally:
        await app.state.runtime.stop()
        app.state.runtime = None


@pytest.mark.asyncio
class TestCampaignConcurrencyCap:
    @pytest.mark.parametrize("mode", ["progressive", "predictive"])
    async def test_concurrent_dial_requests_never_exceed_cap(self, api_without_workers, mode: str) -> None:
        client = api_without_workers
        cid = await _campaign(mode=mode, cap=3, agents=10, borrowers=30)

        responses = await asyncio.gather(*[
            client.post(f"/campaigns/{cid}/dial", params={"provider_id": "mock_provider_a"})
            for _ in range(8)
        ])

        assert all(r.status_code == 200 for r in responses), [r.text for r in responses]
        bodies = [r.json() for r in responses]
        assert sum(b["calls_allocated"] for b in bodies) <= 3
        assert await _active_calls(cid) <= 3
        if mode == "progressive":
            # With 10 agents and 30 borrowers the cap is the binding limit.
            assert await _active_calls(cid) == 3
        for b in bodies:
            d = b["safety_decision"]
            assert d["allocated_count"] == b["calls_allocated"] <= d["approved_count"]

        # The audit trail agrees with what was allocated.
        async with AsyncSessionLocal() as s:
            records = (await s.execute(
                select(SafetyDecisionRecord).where(SafetyDecisionRecord.campaign_id == cid)
            )).scalars().all()
        assert sum(r.approved_count for r in records) <= 3
        assert sum((r.capacity_snapshot or {}).get("allocated_count", 0) for r in records) == await _active_calls(cid)

    @pytest.mark.parametrize("mode", ["progressive", "predictive"])
    async def test_concurrent_dialing_cycles_never_exceed_cap(self, db_session: AsyncSession, mode: str) -> None:
        cid = await _campaign(mode=mode, cap=4, agents=12, borrowers=40)
        safety = SafetyController()

        async def one_cycle() -> int:
            async with AsyncSessionLocal() as s:
                campaign = await s.get(Campaign, cid)
                result = await run_dialing_cycle(s, campaign, provider_id="mock_provider_a", safety=safety)
                await s.commit()
                return len(result.calls)

        allocated = await asyncio.gather(*[one_cycle() for _ in range(10)])
        assert sum(allocated) == await _active_calls(cid)
        assert await _active_calls(cid) <= 4

    async def test_direct_allocator_calls_never_exceed_cap(self, db_session: AsyncSession) -> None:
        """The cap is enforced inside the allocator's transaction, so even callers
        that skip the Safety Controller cannot exceed it."""
        cid = await _campaign(mode="progressive", cap=2, agents=8, borrowers=8)
        allocator = CallAllocator(provider_id="mock_provider_a")
        refused: list[str] = []

        async def attempt() -> None:
            async with AsyncSessionLocal() as s:
                try:
                    await allocator.allocate_one(s, cid)
                    await s.commit()
                except AllocationError as exc:
                    await s.rollback()
                    refused.append(type(exc).__name__)

        await asyncio.gather(*[attempt() for _ in range(8)])
        assert await _active_calls(cid) == 2
        assert refused and set(refused) == {"CampaignConcurrencyLimitError"}

    async def test_cap_frees_up_when_calls_finish(self, db_session: AsyncSession) -> None:
        cid = await _campaign(mode="progressive", cap=1, agents=3, borrowers=3)
        allocator = CallAllocator()
        async with AsyncSessionLocal() as s:
            assert len(await allocator.allocate_batch(s, cid, 3)) == 1
            await s.commit()
        async with AsyncSessionLocal() as s:
            call = (await s.execute(select(Call).where(Call.campaign_id == cid))).scalar_one()
            call.state = "FAILED"
            await s.commit()
        async with AsyncSessionLocal() as s:
            assert len(await allocator.allocate_batch(s, cid, 3)) == 1
            await s.commit()
        assert await _active_calls(cid) == 1
