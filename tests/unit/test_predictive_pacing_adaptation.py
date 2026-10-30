"""Predictive pacing: adapts to observed outcomes and provider health, uses agent
availability correctly, and never gets past the Safety Controller's limits."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import ACTIVE_CALL_STATES, AgentState, CallState, SafetyDecision
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Call, Campaign
from app.services.call_allocator import CallAllocator
from app.services.predictive_dialer import PredictiveDialer
from app.services.predictive_pacer import PredictivePacingEngine
from app.services.provider_health import ProviderHealthService, provider_health_factor
from app.services.safety_controller import SafetyController
from test_safety_accuracy import RunawayPacer


async def _campaign(*, available: int, borrowers: int = 0, cap: int = 50, **agent_states: int) -> str:
    cid = str(uuid.uuid4())
    async with AsyncSessionLocal() as s:
        s.add(Campaign(id=cid, name="pacing", status="ACTIVE", dialing_mode="predictive", max_concurrent_calls=cap))
        await s.flush()
        for i in range(available):
            s.add(Agent(campaign_id=cid, name=f"A{i}", state=AgentState.AVAILABLE))
        for state, n in agent_states.items():
            for i in range(n):
                s.add(Agent(campaign_id=cid, name=f"{state}{i}", state=state.upper()))
        for i in range(borrowers):
            s.add(Borrower(campaign_id=cid, synthetic_name=f"B{i}", masked_phone=f"***-{i}", status="QUEUED"))
        await s.commit()
    return cid


async def _history(cid: str, outcomes: list[bool]) -> None:
    """Finished calls the provider placed, oldest first (True = answered)."""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as s:
        agent = (await s.execute(select(Agent).where(Agent.campaign_id == cid))).scalars().first()
        borrower = Borrower(campaign_id=cid, synthetic_name="H", masked_phone="***-H", status="COMPLETED")
        s.add(borrower)
        await s.flush()
        for i, answered in enumerate(outcomes):
            s.add(Call(campaign_id=cid, agent_id=agent.id, borrower_id=borrower.id,
                       provider_call_id=f"h-{cid[:6]}-{i}", idempotency_key=f"h-{cid}-{i}",
                       state=CallState.COMPLETED if answered else CallState.FAILED,
                       answered_at=now if answered else None,
                       completed_at=now - timedelta(seconds=len(outcomes) - i)))
        await s.commit()


async def _recommend(cid: str, **kwargs):
    pacer = PredictivePacingEngine()
    async with AsyncSessionLocal() as s:
        await pacer.refresh_answer_rate(s, cid)
        return await pacer.recommend(s, cid, "mock_provider_a", **kwargs)


@pytest.mark.asyncio
class TestPacingAdaptation:
    async def test_connected_agents_do_not_suppress_dialing_free_agents(self, db_session: AsyncSession) -> None:
        # Old formula subtracted CONNECTED agents from a target built from AVAILABLE
        # agents: 4 free + 4 talking gave 0.85*4 - 4 → 0, leaving 4 agents idle.
        idle_only = await _recommend(await _campaign(available=4))
        with_talking = await _recommend(await _campaign(available=4, connected=4))
        assert with_talking.recommended_count == idle_only.recommended_count > 0

    async def test_calls_holding_their_own_agent_do_not_suppress_dialing(self, db_session: AsyncSession) -> None:
        cid = await _campaign(available=4, dialing=2)
        async with AsyncSessionLocal() as s:
            dialing_agents = (await s.execute(
                select(Agent).where(Agent.campaign_id == cid, Agent.state == AgentState.DIALING))).scalars().all()
            borrower = Borrower(campaign_id=cid, synthetic_name="R", masked_phone="***-R", status="ALLOCATED")
            s.add(borrower)
            await s.flush()
            for i, agent in enumerate(dialing_agents):
                s.add(Call(campaign_id=cid, agent_id=agent.id, borrower_id=borrower.id,
                           state=CallState.RINGING, idempotency_key=f"ring-{cid}-{i}"))
            await s.commit()
        rec = await _recommend(cid)
        assert rec.outstanding_calls == 2
        assert rec.expected_answers_from_outstanding == 0.0
        assert rec.recommended_count == (await _recommend(await _campaign(available=4))).recommended_count

    async def test_recommendation_follows_observed_answer_rate(self, db_session: AsyncSession) -> None:
        high_cid = await _campaign(available=10)
        await _history(high_cid, [True] * 40)
        low_cid = await _campaign(available=10)
        await _history(low_cid, [i % 10 == 0 for i in range(40)])

        high, low = await _recommend(high_cid), await _recommend(low_cid)
        assert high.answer_rate_confidence == low.answer_rate_confidence == "HIGH"
        assert high.answer_rate_estimate > 0.9 and low.answer_rate_estimate < 0.3
        # Most calls answer: fewer dials reach the utilization target (0.85 × 10).
        assert high.recommended_count < low.recommended_count
        # Few calls answer: dial every free agent — but never more (1:1 reservation).
        assert low.recommended_count == 10

    async def test_cold_start_is_conservative_until_outcomes_are_observed(self, db_session: AsyncSession) -> None:
        cold_cid = await _campaign(available=10)
        warm_cid = await _campaign(available=10)
        await _history(warm_cid, [i % 2 == 0 for i in range(10)])
        cold, warm = await _recommend(cold_cid), await _recommend(warm_cid)
        assert cold.answer_rate_confidence == "LOW" and warm.answer_rate_confidence != "LOW"
        assert cold.recommended_count < warm.recommended_count

    async def test_provider_failures_reduce_then_stop_pacing(self, db_session: AsyncSession) -> None:
        cid = await _campaign(available=8, borrowers=20)
        health = ProviderHealthService("mock_provider_a")
        safety = SafetyController(provider_health_services={"mock_provider_a": health})
        counts = []
        for failures in (0, 3, 2):  # cumulative: 0, 3 (degraded), 5 (circuit opens)
            async with AsyncSessionLocal() as s:
                for _ in range(failures):
                    await health.record_failure(s)
                await s.commit()
            pacer = PredictivePacingEngine()
            async with AsyncSessionLocal() as s:
                await pacer.refresh_answer_rate(s, cid)
                factor = provider_health_factor(await health.get_snapshot(s))
                rec = await pacer.recommend(s, cid, "mock_provider_a", provider_health_factor=factor)
            counts.append(rec.recommended_count)
        assert counts[0] > counts[1] > 0 and counts[2] == 0

        async with AsyncSessionLocal() as s:
            calls, decision, rec = await PredictiveDialer(safety, CallAllocator()).run_cycle(
                s, cid, "mock_provider_a", health_service=health)
            await s.commit()
        assert calls == [] and rec.recommended_count == 0

    async def test_runaway_pacer_cannot_exceed_safety_limits(self, db_session: AsyncSession) -> None:
        cid = await _campaign(available=6, borrowers=20, cap=3)
        async with AsyncSessionLocal() as s:
            calls, decision, _ = await PredictiveDialer(SafetyController(), CallAllocator(), RunawayPacer()).run_cycle(
                s, cid, "mock_provider_a")
            await s.commit()
        assert decision.decision == SafetyDecision.REDUCE
        assert decision.approved_count == 3 and len(calls) == 3
        async with AsyncSessionLocal() as s:
            active = (await s.execute(select(func.count(Call.id)).where(
                Call.campaign_id == cid, Call.state.in_(list(ACTIVE_CALL_STATES))))).scalar_one()
        assert active == 3
