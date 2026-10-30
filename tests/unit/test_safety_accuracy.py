"""Regression tests: safety decisions match what was actually allocated, and an
approval is never given without an audit record."""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, SafetyDecision
from app.core.errors import SafetyViolationError
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Call, Campaign, SafetyDecisionRecord
from app.persistence.repositories import SafetyDecisionRepository
from app.services.call_allocator import CallAllocator
from app.services.dialing import run_dialing_cycle
from app.services.predictive_dialer import PredictiveDialer
from app.services.predictive_pacer import PacingRecommendation, PredictivePacingEngine
from app.services.safety_controller import SafetyController, SafetyDecisionResult


async def _campaign(mode: str, *, agents: int, borrowers: int, cap: int = 10) -> str:
    cid = str(uuid.uuid4())
    async with AsyncSessionLocal() as s:
        s.add(Campaign(id=cid, name=f"acc-{mode}", status="ACTIVE", dialing_mode=mode, max_concurrent_calls=cap))
        await s.flush()
        for i in range(agents):
            s.add(Agent(campaign_id=cid, name=f"A{i}", state=AgentState.AVAILABLE))
        for i in range(borrowers):
            s.add(Borrower(campaign_id=cid, synthetic_name=f"B{i}", masked_phone=f"***-{i}", status="QUEUED"))
        await s.commit()
    return cid


async def _dial(cid: str, safety: SafetyController):
    async with AsyncSessionLocal() as s:
        result = await run_dialing_cycle(s, await s.get(Campaign, cid), provider_id="mock_provider_a", safety=safety)
        await s.commit()
    return result


async def _records(cid: str) -> list[SafetyDecisionRecord]:
    async with AsyncSessionLocal() as s:
        return list((await s.execute(
            select(SafetyDecisionRecord).where(SafetyDecisionRecord.campaign_id == cid)
        )).scalars().all())


class RunawayPacer(PredictivePacingEngine):
    """A broken pacer that always recommends far more calls than is safe."""

    async def recommend(self, session, campaign_id, provider_id, *, provider_health_factor=1.0):
        return PacingRecommendation(
            campaign_id=campaign_id, recommended_count=1000, answer_rate_estimate=0.5,
            answer_rate_confidence="LOW", outstanding_calls=0, expected_answers_from_outstanding=0.0,
            available_agents=0, provider_health_factor=provider_health_factor,
        )


@pytest.mark.asyncio
class TestSafetyDecisionAccuracy:
    @pytest.mark.parametrize("mode", ["progressive", "predictive"])
    async def test_approval_matches_allocation_when_borrowers_run_out(
        self, db_session: AsyncSession, mode: str
    ) -> None:
        # Previously: 5 agents → "APPROVE 5" but only 2 calls (2 borrowers).
        cid = await _campaign(mode, agents=5, borrowers=2)
        result = await _dial(cid, SafetyController())
        assert len(result.calls) == 2
        assert result.decision.requested_count == result.decision.approved_count == 2
        assert result.decision.allocated_count == 2
        [record] = await _records(cid)
        assert (record.requested_count, record.approved_count) == (2, 2)
        assert record.capacity_snapshot["allocated_count"] == 2
        assert not any(r.startswith("ALLOCATION_SHORTFALL") for r in record.reason_codes)

    @pytest.mark.parametrize("mode", ["progressive", "predictive"])
    async def test_no_eligible_borrowers_is_not_an_approval(self, db_session: AsyncSession, mode: str) -> None:
        cid = await _campaign(mode, agents=3, borrowers=0)
        result = await _dial(cid, SafetyController())
        assert result.calls == []
        assert result.decision.approved_count == 0
        assert "NO_ELIGIBLE_BORROWERS" in result.decision.reason_codes
        assert result.decision.persisted is False  # Safety Controller not consulted
        assert await _records(cid) == []

    async def test_shortfall_against_approval_is_recorded(self, db_session: AsyncSession) -> None:
        # Permissive mode approves by dial ratio (3 per agent), but every call
        # still needs its own agent: approval 6, allocation 2, cause recorded.
        cid = await _campaign("predictive", agents=2, borrowers=10)
        safety = SafetyController(strict_mode=False, max_dial_ratio=3.0)
        async with AsyncSessionLocal() as s:
            calls, decision, _ = await PredictiveDialer(safety, CallAllocator(), RunawayPacer()).run_cycle(
                s, cid, "mock_provider_a")
            await s.commit()
        assert decision.requested_count == 10  # capped by eligible borrowers, not 1000
        assert decision.approved_count == 6
        assert decision.allocated_count == len(calls) == 2
        [record] = await _records(cid)
        assert record.approved_count == 6
        assert record.capacity_snapshot["allocated_count"] == 2
        assert "ALLOCATION_SHORTFALL:NO_AVAILABLE_AGENT" in record.reason_codes

    async def test_audit_failure_fails_closed(self, db_session: AsyncSession, monkeypatch) -> None:
        cid = await _campaign("progressive", agents=3, borrowers=3)

        async def broken_create(self, record):
            raise RuntimeError("audit table unavailable")

        monkeypatch.setattr(SafetyDecisionRepository, "create", broken_create)
        result = await _dial(cid, SafetyController())
        assert result.decision.decision == SafetyDecision.REJECT
        assert result.decision.approved_count == 0
        assert any(r.startswith("AUDIT_PERSIST_FAILED") for r in result.decision.reason_codes)
        assert result.calls == []
        async with AsyncSessionLocal() as s:
            assert (await s.execute(select(Call).where(Call.campaign_id == cid))).first() is None

    async def test_allocation_beyond_approval_is_refused(self, db_session: AsyncSession) -> None:
        decision = SafetyDecisionResult(campaign_id="c", requested_count=2, approved_count=2,
                                        decision=SafetyDecision.APPROVE)
        async with AsyncSessionLocal() as s:
            with pytest.raises(SafetyViolationError):
                await SafetyController().record_allocation(s, decision, allocated=3)

    async def test_api_lists_allocated_count(self, db_session: AsyncSession) -> None:
        cid = await _campaign("progressive", agents=2, borrowers=5)
        await _dial(cid, SafetyController())
        async with AsyncSessionLocal() as s:
            from app.api.campaigns import get_safety_decisions
            [entry] = await get_safety_decisions(cid, session=s)
        assert entry["approved"] == entry["allocated"] == 2
