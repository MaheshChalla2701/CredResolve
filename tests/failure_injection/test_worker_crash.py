"""Failure injection tests: worker crash recovery and lease expiration."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import CallState, DispatchStatus
from app.persistence.models import Agent, Borrower, Campaign, DispatchRecord
from app.services.call_allocator import CallAllocator
from app.services.reconciliation import ReconciliationService


@pytest.mark.asyncio
class TestWorkerCrashRecovery:
    async def test_reclaims_stale_dispatch_lease(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        allocator = CallAllocator(provider_id="mock_provider_a")
        call = await allocator.allocate_one(db_session, sample_campaign.id)
        await db_session.commit()

        # Simulate worker claimed outbox record, but crashed while processing (lease expired)
        from app.persistence.repositories import DispatchRepository
        dispatch_repo = DispatchRepository(db_session)
        dispatch = await dispatch_repo.get_by_call_id(call.id)
        assert dispatch is not None

        dispatch.status = DispatchStatus.CLAIMED
        dispatch.worker_id = "crashed-worker-99"
        # Backdate lease expiration past threshold
        dispatch.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=120)
        call.created_at = datetime.now(timezone.utc) - timedelta(seconds=180)
        await db_session.commit()

        # Run reconciliation
        reconciler = ReconciliationService()
        await reconciler.run(db_session)
        await db_session.commit()

        # Verify call is safely handled (marked AMBIGUOUS because lease expired with no confirmation)
        await db_session.refresh(call)
        assert call.is_ambiguous is True
