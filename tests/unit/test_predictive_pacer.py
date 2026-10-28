"""Unit tests for the Predictive Pacing Engine and Answer Rate Estimator."""
from __future__ import annotations

import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, CallState
from app.persistence.models import Agent, Borrower, Call, Campaign
from app.services.predictive_pacer import AnswerRateEstimator, PredictivePacingEngine


class TestAnswerRateEstimator:
    def test_initial_prior(self) -> None:
        """Prior Beta(2, 2) defaults to 0.5 with LOW confidence."""
        est = AnswerRateEstimator(min_samples=5)
        assert est.estimate == 0.5
        assert est.confidence == "LOW"
        assert est.sample_count == 0

    def test_confidence_escalation(self) -> None:
        """Confidence transitions from LOW -> MEDIUM -> HIGH as samples accumulate."""
        est = AnswerRateEstimator(min_samples=5)
        assert est.confidence == "LOW"

        for _ in range(5):
            est.observe(True)
        assert est.confidence == "MEDIUM"

        for _ in range(25):
            est.observe(True)
        assert est.confidence == "HIGH"

    def test_ewma_tracking(self) -> None:
        """Estimator responds dynamically to observations with configured alpha."""
        est = AnswerRateEstimator(smoothing_alpha=0.5)
        # Prior is 0.5
        # Observe False: 0.5 * 0 + 0.5 * 0.5 = 0.25
        est.observe(False)
        assert est.estimate == 0.25

        # Observe True: 0.5 * 1.0 + 0.5 * 0.25 = 0.625
        est.observe(True)
        assert est.estimate == 0.625

    def test_clamping(self) -> None:
        """Estimates remain strictly within (0.01, 0.99) boundary."""
        est = AnswerRateEstimator(smoothing_alpha=1.0)
        est.observe(False)
        assert est.estimate >= 0.01

        est.observe(True)
        assert est.estimate <= 0.99


@pytest.mark.asyncio
class TestPredictivePacingEngine:
    async def test_zero_available_agents_recommends_zero(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        pacer = PredictivePacingEngine()
        rec = await pacer.recommend(db_session, sample_campaign.id, "mock_provider_a")
        assert rec.recommended_count == 0
        assert rec.available_agents == 0

    async def test_recommends_within_available_capacity(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        # Add 5 available agents
        for i in range(5):
            agent = Agent(
                id=str(uuid.uuid4()),
                campaign_id=sample_campaign.id,
                name=f"Agent-{i}",
                state=AgentState.AVAILABLE,
            )
            db_session.add(agent)
        await db_session.commit()

        pacer = PredictivePacingEngine(utilization_target=0.8)
        rec = await pacer.recommend(db_session, sample_campaign.id, "mock_provider_a")

        # 5 available * 0.8 = 4 desired
        assert rec.available_agents == 5
        assert rec.recommended_count <= 5
        assert rec.recommended_count >= 1

    async def test_outstanding_calls_dampen_recommendation(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        # 1 agent available, but 2 calls already RINGING in-flight
        for i in range(2):
            call = Call(
                id=str(uuid.uuid4()),
                campaign_id=sample_campaign.id,
                agent_id=sample_agent.id,
                borrower_id=sample_borrower.id,
                state=CallState.RINGING,
                idempotency_key=f"pacer-test-{i}",
            )
            db_session.add(call)
        await db_session.commit()

        pacer = PredictivePacingEngine()
        rec = await pacer.recommend(db_session, sample_campaign.id, "mock_provider_a")

        # Expected answers from outstanding calls reduce recommendation to 0
        assert rec.outstanding_calls == 2
        assert rec.recommended_count == 0

    async def test_degraded_provider_scales_down_proposal(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        for i in range(10):
            agent = Agent(
                id=str(uuid.uuid4()),
                campaign_id=sample_campaign.id,
                name=f"Agent-{i}",
                state=AgentState.AVAILABLE,
            )
            db_session.add(agent)
        await db_session.commit()

        pacer = PredictivePacingEngine()
        # Normal healthy provider recommendation
        rec_healthy = await pacer.recommend(
            db_session, sample_campaign.id, "mock_provider_a", provider_health_factor=1.0
        )
        # Degraded provider health factor (e.g. 0.2)
        rec_degraded = await pacer.recommend(
            db_session, sample_campaign.id, "mock_provider_a", provider_health_factor=0.2
        )

        assert rec_degraded.recommended_count <= rec_healthy.recommended_count
        assert rec_degraded.provider_health_factor == 0.2
