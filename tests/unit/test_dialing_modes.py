"""Regression tests: campaign dialing mode selection and Safety Controller
enforcement in both progressive and predictive modes."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, CallState, SafetyDecision
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Call, Campaign, ProviderHealthRecord
from app.services.dialing import run_dialing_cycle
from app.services.predictive_pacer import PredictivePacingEngine
from app.services.provider_health import ProviderHealthService
from app.services.safety_controller import SafetyController


async def _campaign(mode: str, *, agents: int, borrowers: int, max_concurrent: int = 10) -> str:
    cid = str(uuid.uuid4())
    async with AsyncSessionLocal() as s:
        s.add(Campaign(id=cid, name=f"modes-{mode}", status="ACTIVE", dialing_mode=mode,
                       max_concurrent_calls=max_concurrent))
        await s.flush()
        for i in range(agents):
            s.add(Agent(campaign_id=cid, name=f"A{i}", state=AgentState.AVAILABLE))
        for i in range(borrowers):
            s.add(Borrower(campaign_id=cid, synthetic_name=f"B{i}", masked_phone=f"***-{i}", status="QUEUED"))
        await s.commit()
    return cid


async def _dial(cid: str, safety: SafetyController, health: ProviderHealthService | None = None):
    async with AsyncSessionLocal() as s:
        campaign = await s.get(Campaign, cid)
        result = await run_dialing_cycle(s, campaign, provider_id="mock_provider_a", safety=safety,
                                         health_service=health)
        await s.commit()
    return result


async def _set_circuit(provider_id: str, state: str, opened_ago: float = 0.0) -> ProviderHealthService:
    async with AsyncSessionLocal() as s:
        s.add(ProviderHealthRecord(provider_id=provider_id, circuit_state=state, consecutive_failures=5,
                                   circuit_opened_at=datetime.now(timezone.utc) - timedelta(seconds=opened_ago)))
        await s.commit()
    return ProviderHealthService(provider_id)


@pytest.mark.asyncio
class TestDialingModes:
    async def test_progressive_campaign_uses_progressive_dialer(self, db_session: AsyncSession) -> None:
        cid = await _campaign("progressive", agents=3, borrowers=5)
        result = await _dial(cid, SafetyController())
        assert result.mode == "progressive" and result.recommendation is None
        assert result.decision.requested_count == 3
        assert len(result.calls) == 3

    async def test_predictive_campaign_uses_pacer_then_safety(self, db_session: AsyncSession) -> None:
        cid = await _campaign("predictive", agents=4, borrowers=8)
        result = await _dial(cid, SafetyController())
        assert result.mode == "predictive"
        assert result.recommendation is not None
        assert result.decision.requested_count == result.recommendation.recommended_count
        assert 0 < len(result.calls) <= result.decision.approved_count <= 4

    async def test_predictive_proposal_is_capped_by_safety_limits(self, db_session: AsyncSession) -> None:
        cid = await _campaign("predictive", agents=5, borrowers=10, max_concurrent=1)
        result = await _dial(cid, SafetyController())
        assert result.recommendation.recommended_count >= 1
        if result.recommendation.recommended_count > 1:
            assert result.decision.decision == SafetyDecision.REDUCE
        assert result.decision.approved_count == 1
        assert len(result.calls) == 1

    @pytest.mark.parametrize("mode", ["progressive", "predictive"])
    async def test_open_circuit_blocks_dialing_in_both_modes(self, db_session: AsyncSession, mode: str) -> None:
        cid = await _campaign(mode, agents=3, borrowers=3)
        health = await _set_circuit("mock_provider_a", "OPEN")
        safety = SafetyController(provider_health_services={"mock_provider_a": health})
        # No health_service for the pacer here: the Safety Controller alone must block.
        result = await _dial(cid, safety)
        assert result.decision.decision == SafetyDecision.REJECT
        assert "CIRCUIT_BREAKER_OPEN" in result.decision.reason_codes
        assert result.calls == []

    async def test_pacer_stops_proposing_when_provider_is_down(self, db_session: AsyncSession) -> None:
        cid = await _campaign("predictive", agents=3, borrowers=3)
        health = await _set_circuit("mock_provider_a", "OPEN")
        result = await _dial(cid, SafetyController(provider_health_services={"mock_provider_a": health}), health)
        assert result.recommendation.provider_health_factor == 0.0
        assert result.recommendation.recommended_count == 0
        assert result.calls == []

    async def test_half_open_circuit_allows_limited_probes(self, db_session: AsyncSession) -> None:
        cid = await _campaign("progressive", agents=5, borrowers=5)
        # Opened long ago: the cooldown has elapsed, so the snapshot moves it to HALF_OPEN.
        health = await _set_circuit("mock_provider_a", "OPEN", opened_ago=3600)
        result = await _dial(cid, SafetyController(provider_health_services={"mock_provider_a": health}))
        assert result.decision.provider_health_snapshot["circuit_state"] == "HALF_OPEN"
        assert "CIRCUIT_HALF_OPEN_PROBE_LIMIT" in result.decision.reason_codes
        assert len(result.calls) == 2  # circuit_breaker_half_open_probes

    async def test_pacer_learns_answer_rate_from_call_history(self, db_session: AsyncSession) -> None:
        cid = await _campaign("predictive", agents=1, borrowers=1)
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as s:
            agent_id = (await s.execute(Agent.__table__.select().where(Agent.campaign_id == cid))).first().id
            borrower_id = (await s.execute(Borrower.__table__.select().where(Borrower.campaign_id == cid))).first().id
            for i in range(20):
                answered = i % 5 == 0  # 20% answered
                s.add(Call(campaign_id=cid, agent_id=agent_id, borrower_id=borrower_id,
                           provider_call_id=f"hist-{i}", idempotency_key=f"hist-{cid}-{i}",
                           state=CallState.COMPLETED if answered else CallState.FAILED,
                           answered_at=now if answered else None,
                           completed_at=now - timedelta(seconds=20 - i)))
            await s.commit()

        pacer = PredictivePacingEngine()
        async with AsyncSessionLocal() as s:
            observed = await pacer.refresh_answer_rate(s, cid)
        assert observed == 20
        assert pacer.estimator.confidence == "MEDIUM"
        assert pacer.estimator.estimate < 0.4  # moved well below the 0.5 prior
