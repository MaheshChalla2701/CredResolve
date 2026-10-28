"""Unit tests for the Independent Safety Controller."""
from __future__ import annotations

import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, CampaignStatus, CircuitBreakerState, SafetyDecision
from app.persistence.models import Agent, Campaign
from app.services.safety_controller import SafetyController


class MockCircuitBreaker:
    def __init__(self, state: CircuitBreakerState = CircuitBreakerState.CLOSED) -> None:
        self.state = state

    async def can_attempt(self) -> bool:
        return self.state != CircuitBreakerState.OPEN

    async def get_snapshot(self, session) -> dict:
        return {"circuit_state": self.state}


@pytest.mark.asyncio
class TestSafetyController:
    async def test_rejects_zero_or_negative_request(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        controller = SafetyController()
        decision = await controller.evaluate(
            db_session,
            campaign_id=sample_campaign.id,
            requested_count=0,
            provider_id="mock_provider_a",
        )
        assert decision.decision == SafetyDecision.REJECT
        assert decision.approved_count == 0
        assert "REQUESTED_COUNT_ZERO" in decision.reason_codes

        neg_decision = await controller.evaluate(
            db_session,
            campaign_id=sample_campaign.id,
            requested_count=-5,
            provider_id="mock_provider_a",
        )
        assert neg_decision.decision == SafetyDecision.REJECT
        assert neg_decision.approved_count == 0

    async def test_rejects_when_campaign_inactive(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        sample_campaign.status = CampaignStatus.PAUSED
        await db_session.commit()

        controller = SafetyController()
        decision = await controller.evaluate(
            db_session,
            campaign_id=sample_campaign.id,
            requested_count=5,
            provider_id="mock_provider_a",
        )
        assert decision.decision == SafetyDecision.REJECT
        assert decision.approved_count == 0
        assert any("CAMPAIGN_NOT_ACTIVE" in r for r in decision.reason_codes)

    async def test_strict_mode_rejects_with_no_available_agents(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        # No agents in DB
        controller = SafetyController(strict_mode=True)
        decision = await controller.evaluate(
            db_session,
            campaign_id=sample_campaign.id,
            requested_count=5,
            provider_id="mock_provider_a",
        )
        assert decision.decision == SafetyDecision.REJECT
        assert decision.approved_count == 0
        assert "NO_AVAILABLE_AGENT_CAPACITY" in decision.reason_codes

    async def test_strict_mode_caps_at_available_agents(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        # Add 3 available agents
        for i in range(3):
            agent = Agent(
                id=str(uuid.uuid4()),
                campaign_id=sample_campaign.id,
                name=f"Agent-{i}",
                state=AgentState.AVAILABLE,
            )
            db_session.add(agent)
        await db_session.commit()

        controller = SafetyController(strict_mode=True)
        # Request 10 calls with only 3 agents available
        decision = await controller.evaluate(
            db_session,
            campaign_id=sample_campaign.id,
            requested_count=10,
            provider_id="mock_provider_a",
        )
        assert decision.decision == SafetyDecision.REDUCE
        assert decision.approved_count == 3

    async def test_rejects_when_circuit_breaker_open(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent
    ) -> None:
        cb = MockCircuitBreaker(state=CircuitBreakerState.OPEN)
        controller = SafetyController(provider_health_services={"mock_provider_a": cb})

        decision = await controller.evaluate(
            db_session,
            campaign_id=sample_campaign.id,
            requested_count=1,
            provider_id="mock_provider_a",
        )
        assert decision.decision == SafetyDecision.REJECT
        assert decision.approved_count == 0
        assert "CIRCUIT_BREAKER_OPEN" in decision.reason_codes

    async def test_fails_closed_on_db_error(
        self, sample_campaign: Campaign
    ) -> None:
        # Pass an uninitialized or broken session
        broken_session = None  # will cause exception inside evaluate
        controller = SafetyController()
        decision = await controller.evaluate(
            broken_session,  # type: ignore
            campaign_id=sample_campaign.id,
            requested_count=5,
            provider_id="mock_provider_a",
        )
        assert decision.decision == SafetyDecision.REJECT
        assert decision.approved_count == 0
        assert any("INTERNAL_ERROR" in r for r in decision.reason_codes)
