"""Integration tests for Atomic Call Allocator."""
from __future__ import annotations

import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, BorrowerStatus, CallState, DispatchStatus
from app.core.errors import NoAvailableAgentError, NoEligibleBorrowerError
from app.persistence.models import Agent, Borrower, Campaign, DispatchRecord
from app.services.call_allocator import CallAllocator


@pytest.mark.asyncio
class TestCallAllocator:
    async def test_successful_single_allocation(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        allocator = CallAllocator(provider_id="mock_provider_a")
        call = await allocator.allocate_one(db_session, sample_campaign.id)
        await db_session.commit()

        assert call is not None
        assert call.campaign_id == sample_campaign.id
        assert call.agent_id == sample_agent.id
        assert call.borrower_id == sample_borrower.id
        assert call.state == CallState.RESERVED
        assert call.attempt_number == 1
        assert call.idempotency_key == f"alloc:{sample_campaign.id}:{sample_borrower.id}:1"

        # Verify agent transitioned to RESERVED
        await db_session.refresh(sample_agent)
        assert sample_agent.state == AgentState.RESERVED
        assert sample_agent.current_call_id == call.id

        # Verify borrower transitioned to ALLOCATED with attempt incremented
        await db_session.refresh(sample_borrower)
        assert sample_borrower.status == BorrowerStatus.ALLOCATED
        assert sample_borrower.attempt_count == 1

        # Verify dispatch outbox record written
        from app.persistence.repositories import DispatchRepository
        dispatch_repo = DispatchRepository(db_session)
        dispatch = await dispatch_repo.get_by_call_id(call.id)
        assert dispatch is not None
        assert dispatch.status == DispatchStatus.PENDING
        assert dispatch.idempotency_key == call.idempotency_key

    async def test_no_available_agent_raises(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_borrower: Borrower,
    ) -> None:
        allocator = CallAllocator()
        with pytest.raises(NoAvailableAgentError):
            await allocator.allocate_one(db_session, sample_campaign.id)

    async def test_no_eligible_borrower_raises(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
    ) -> None:
        allocator = CallAllocator()
        with pytest.raises(NoEligibleBorrowerError):
            await allocator.allocate_one(db_session, sample_campaign.id)

    async def test_batch_allocation(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
    ) -> None:
        # Add 3 agents
        for i in range(3):
            db_session.add(
                Agent(
                    id=str(uuid.uuid4()),
                    campaign_id=sample_campaign.id,
                    name=f"BatchAgent-{i}",
                    state=AgentState.AVAILABLE,
                )
            )
        # Add 5 borrowers
        for j in range(5):
            db_session.add(
                Borrower(
                    id=str(uuid.uuid4()),
                    campaign_id=sample_campaign.id,
                    synthetic_name=f"Borrower-{j}",
                    masked_phone=f"+1555111{j:04d}",
                    status=BorrowerStatus.QUEUED,
                )
            )
        await db_session.commit()

        allocator = CallAllocator(provider_id="mock_provider_a")
        # Request batch of 2
        calls = await allocator.allocate_batch(db_session, sample_campaign.id, count=2)
        await db_session.commit()

        assert len(calls) == 2
        for c in calls:
            assert c.state == CallState.RESERVED
