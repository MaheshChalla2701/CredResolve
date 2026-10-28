"""Failure injection tests: provider timeouts, 5xx outages, and circuit breaker trip."""
from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import CircuitBreakerState, DispatchStatus
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Campaign, DispatchRecord
from app.providers.mock_provider_a import MockProviderA
from app.services.call_allocator import CallAllocator
from app.services.provider_health import ProviderHealthService
from app.workers.dispatch_worker import DispatchWorker


@pytest.mark.asyncio
class TestProviderFailureInjection:
    async def test_provider_outage_handling(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        allocator = CallAllocator(provider_id="mock_provider_a")
        call = await allocator.allocate_one(db_session, sample_campaign.id)
        await db_session.commit()

        # Simulate total provider outage
        provider = MockProviderA(simulate_outage=True)
        worker = DispatchWorker(provider=provider, worker_id="test-fail-worker")

        # Run dispatch worker
        processed = await worker.run_once(AsyncSessionLocal, batch_size=5)
        assert processed == 1

        # Verify dispatch record marked FAILED with outage error detail
        from app.persistence.repositories import DispatchRepository
        dispatch_repo = DispatchRepository(db_session)
        dispatch = await dispatch_repo.get_by_call_id(call.id)
        assert dispatch is not None
        assert dispatch.status == DispatchStatus.FAILED
        assert dispatch.result_code == "PROVIDER_OUTAGE"

    async def test_circuit_breaker_tripping_on_repeated_failures(
        self,
        db_session: AsyncSession,
    ) -> None:
        health_svc = ProviderHealthService(provider_id="flaky_provider")

        # Record 5 consecutive failures to trip threshold
        for _ in range(5):
            await health_svc.record_failure(db_session)
            await db_session.commit()

        snapshot = await health_svc.get_snapshot(db_session)
        assert snapshot["circuit_state"] == CircuitBreakerState.OPEN
        assert await health_svc.is_available(db_session) is False
