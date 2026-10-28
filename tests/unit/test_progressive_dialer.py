"""Unit tests for Progressive Dialer."""
from __future__ import annotations

import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, BorrowerStatus, SafetyDecision
from app.persistence.models import Agent, Borrower, Campaign
from app.services.call_allocator import CallAllocator
from app.services.progressive_dialer import ProgressiveDialer
from app.services.safety_controller import SafetyController


@pytest.mark.asyncio
class TestProgressiveDialer:
    async def test_no_agents_available_returns_empty(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        safety = SafetyController()
        allocator = CallAllocator()
        dialer = ProgressiveDialer(safety, allocator)

        calls, decision = await dialer.run_cycle(
            db_session, sample_campaign.id, "mock_provider_a"
        )
        assert len(calls) == 0
        assert decision.decision == SafetyDecision.REJECT
        assert "NO_AVAILABLE_AGENTS" in decision.reason_codes

    async def test_allocates_exact_agent_capacity(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        # Add 3 available agents
        for i in range(3):
            agent = Agent(
                id=str(uuid.uuid4()),
                campaign_id=sample_campaign.id,
                name=f"ProgAgent-{i}",
                state=AgentState.AVAILABLE,
            )
            db_session.add(agent)

        # Add 5 queued borrowers
        for j in range(5):
            borrower = Borrower(
                id=str(uuid.uuid4()),
                campaign_id=sample_campaign.id,
                synthetic_name=f"Borrower-{j}",
                masked_phone=f"+15550000{j:02d}",
                status=BorrowerStatus.QUEUED,
                attempt_count=0,
                max_attempts=3,
            )
            db_session.add(borrower)
        await db_session.commit()

        safety = SafetyController(strict_mode=True)
        allocator = CallAllocator(provider_id="mock_provider_a")
        dialer = ProgressiveDialer(safety, allocator)

        calls, decision = await dialer.run_cycle(
            db_session, sample_campaign.id, "mock_provider_a"
        )
        await db_session.commit()

        # In progressive mode with 3 agents, exactly 3 calls should be allocated
        assert decision.approved_count == 3
        assert len(calls) == 3
        for call in calls:
            assert call.campaign_id == sample_campaign.id
            assert call.state == "RESERVED"

    async def test_max_batch_capping(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        # Add 5 available agents
        for i in range(5):
            agent = Agent(
                id=str(uuid.uuid4()),
                campaign_id=sample_campaign.id,
                name=f"ProgAgent-{i}",
                state=AgentState.AVAILABLE,
            )
            db_session.add(agent)

        for j in range(5):
            borrower = Borrower(
                id=str(uuid.uuid4()),
                campaign_id=sample_campaign.id,
                synthetic_name=f"Borrower-{j}",
                masked_phone=f"+15550000{j:02d}",
                status=BorrowerStatus.QUEUED,
            )
            db_session.add(borrower)
        await db_session.commit()

        safety = SafetyController()
        allocator = CallAllocator()
        dialer = ProgressiveDialer(safety, allocator)

        # Limit cycle batch to 2
        calls, decision = await dialer.run_cycle(
            db_session, sample_campaign.id, "mock_provider_a", max_batch=2
        )
        await db_session.commit()

        assert decision.approved_count == 2
        assert len(calls) == 2
