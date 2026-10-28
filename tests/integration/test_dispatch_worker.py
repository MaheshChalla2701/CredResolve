"""Integration tests for DispatchWorker."""
from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import CallState, DispatchStatus
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Campaign, DispatchRecord
from app.providers.mock_provider_a import MockProviderA
from app.services.call_allocator import CallAllocator
from app.workers.dispatch_worker import DispatchWorker


@pytest.mark.asyncio
class TestDispatchWorker:
    async def test_dispatches_pending_outbox_record(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        # 1. Allocate a call to create a pending dispatch record
        allocator = CallAllocator(provider_id="mock_provider_a")
        call = await allocator.allocate_one(db_session, sample_campaign.id)
        await db_session.commit()

        # 2. Setup mock provider and worker
        provider = MockProviderA(seed=42, min_latency_ms=1.0, max_latency_ms=2.0)
        worker = DispatchWorker(provider=provider, worker_id="test-worker-01")

        # 3. Run worker
        processed = await worker.run_once(AsyncSessionLocal, batch_size=10)
        assert processed == 1

        # 4. Verify call state updated to INITIATED with provider_call_id
        await db_session.refresh(call)
        assert call.state == CallState.INITIATED
        assert call.provider_call_id is not None

        # 5. Verify dispatch record marked SUCCEEDED
        from app.persistence.repositories import DispatchRepository
        dispatch_repo = DispatchRepository(db_session)
        dispatch = await dispatch_repo.get_by_call_id(call.id)
        assert dispatch is not None
        assert dispatch.status == DispatchStatus.SUCCEEDED
        assert dispatch.provider_call_id == call.provider_call_id

    async def test_no_pending_dispatches_returns_zero(
        self, db_session: AsyncSession
    ) -> None:
        provider = MockProviderA()
        worker = DispatchWorker(provider=provider)
        count = await worker.run_once(AsyncSessionLocal, batch_size=5)
        assert count == 0
