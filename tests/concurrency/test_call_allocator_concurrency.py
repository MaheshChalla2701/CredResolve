"""Concurrency tests for Call Allocator under simultaneous worker contention."""
from __future__ import annotations

import asyncio
import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, BorrowerStatus
from app.core.errors import NoAvailableAgentError, NoEligibleBorrowerError
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Call, Campaign
from app.services.call_allocator import CallAllocator


@pytest.mark.asyncio
class TestCallAllocatorConcurrency:
    async def test_concurrent_workers_no_over_allocation(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
    ) -> None:
        """Multiple concurrent tasks compete for 5 available agents. Exactly 5 calls must be allocated."""
        num_agents = 5
        num_borrowers = 15

        for i in range(num_agents):
            db_session.add(
                Agent(
                    id=str(uuid.uuid4()),
                    campaign_id=sample_campaign.id,
                    name=f"ConcAgent-{i}",
                    state=AgentState.AVAILABLE,
                )
            )

        for j in range(num_borrowers):
            db_session.add(
                Borrower(
                    id=str(uuid.uuid4()),
                    campaign_id=sample_campaign.id,
                    synthetic_name=f"Borrower-{j}",
                    masked_phone=f"+1555222{j:04d}",
                    status=BorrowerStatus.QUEUED,
                )
            )
        await db_session.commit()

        allocator = CallAllocator(provider_id="mock_provider_a")
        allocated_calls: list[str] = []
        lock = asyncio.Lock()

        async def worker_attempt() -> None:
            for _ in range(10):
                try:
                    async with AsyncSessionLocal() as session:
                        call = await allocator.allocate_one(session, sample_campaign.id)
                        await session.commit()
                        async with lock:
                            allocated_calls.append(call.id)
                        break
                except (NoAvailableAgentError, NoEligibleBorrowerError):
                    break
                except Exception:
                    await asyncio.sleep(0.05)

        # Spawn 15 concurrent workers for 5 agents
        tasks = [asyncio.create_task(worker_attempt()) for _ in range(15)]
        await asyncio.gather(*tasks)

        # Invariant 1: Exactly 5 calls should be created (since only 5 agents exist)
        assert len(allocated_calls) == num_agents

        # Invariant 2: Check each call in DB has unique agent_id
        async with AsyncSessionLocal() as session:
            from sqlalchemy import select
            result = await session.execute(
                select(Call).where(Call.campaign_id == sample_campaign.id)
            )
            calls = result.scalars().all()
            agent_ids = [c.agent_id for c in calls]
            assert len(agent_ids) == len(set(agent_ids)), "Duplicate agent assignment detected!"
            borrower_ids = [c.borrower_id for c in calls]
            assert len(borrower_ids) == len(set(borrower_ids)), "Duplicate borrower assignment detected!"
