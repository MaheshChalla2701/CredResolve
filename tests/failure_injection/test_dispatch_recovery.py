"""Regression tests: dispatch outcomes, ambiguous results, worker crashes and
lease recovery — without ever placing a duplicate provider call."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, BorrowerStatus, CallState, DispatchStatus
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Call, Campaign, DispatchRecord
from app.persistence.repositories import DispatchRepository
from app.providers.mock_provider_a import MockProviderA
from app.services.call_allocator import CallAllocator
from app.services.reconciliation import ReconciliationService
from app.workers.dispatch_worker import DispatchWorker
from conftest import StubProvider, allocate_and_dispatch


async def _snapshot(call_id: str):
    async with AsyncSessionLocal() as s:
        call = await s.get(Call, call_id)
        agent = await s.get(Agent, call.agent_id)
        borrower = await s.get(Borrower, call.borrower_id)
        dispatch = await DispatchRepository(s).get_by_call_id(call_id)
        return call, agent, borrower, dispatch


async def _reconcile(provider, grace: float = 0.0) -> dict:
    async with AsyncSessionLocal() as s:
        result = await ReconciliationService(
            provider_adapters={provider.provider_id: provider}, ambiguous_grace_seconds=grace
        ).run(s)
        await s.commit()
    assert result["errors"] == []
    return result


async def _allocate(campaign_id: str) -> str:
    async with AsyncSessionLocal() as s:
        call = await CallAllocator(provider_id="mock_provider_a").allocate_one(s, campaign_id)
        await s.commit()
        return call.id


@pytest.mark.asyncio
class TestDispatchRecovery:
    async def test_provider_rejection_releases_agent_and_requeues_borrower(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent, sample_borrower: Borrower,
    ) -> None:
        provider = MockProviderA(simulate_outage=True)
        [call_id] = await allocate_and_dispatch(sample_campaign.id, provider)
        call, agent, borrower, dispatch = await _snapshot(call_id)
        assert dispatch.status == DispatchStatus.FAILED and dispatch.result_code == "PROVIDER_OUTAGE"
        assert call.state == CallState.FAILED and call.failure_reason == "PROVIDER_OUTAGE"
        assert agent.state == AgentState.AVAILABLE and agent.current_call_id is None
        assert borrower.status == BorrowerStatus.QUEUED  # previously stuck ALLOCATED

    async def test_timeout_is_ambiguous_and_reconciled_as_not_placed(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent, sample_borrower: Borrower,
    ) -> None:
        provider = MockProviderA(timeout_rate=1.0, min_latency_ms=1, max_latency_ms=2)
        [call_id] = await allocate_and_dispatch(sample_campaign.id, provider)
        call, agent, borrower, dispatch = await _snapshot(call_id)
        assert dispatch.status == DispatchStatus.AMBIGUOUS and dispatch.result_code == "TIMEOUT"
        assert call.state == CallState.INITIATED and call.is_ambiguous
        assert agent.state == AgentState.RESERVED  # held while the outcome is unknown

        result = await _reconcile(provider)
        assert result["ambiguous_resolved"] == 1
        call, agent, borrower, dispatch = await _snapshot(call_id)
        assert call.state == CallState.FAILED and call.failure_reason == "RECONCILE_NOT_PLACED"
        assert not call.is_ambiguous
        assert dispatch.status == DispatchStatus.FAILED
        assert agent.state == AgentState.AVAILABLE
        assert borrower.status == BorrowerStatus.QUEUED

    async def test_timeout_after_acceptance_recovers_provider_call_id(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent, sample_borrower: Borrower,
    ) -> None:
        provider = StubProvider(mode="timeout_after_accept")
        [call_id] = await allocate_and_dispatch(sample_campaign.id, provider)
        await _reconcile(provider)
        call, agent, _, dispatch = await _snapshot(call_id)
        assert call.provider_call_id == provider.accepted[call.idempotency_key]
        assert not call.is_ambiguous and call.state == CallState.INITIATED
        assert dispatch.status == DispatchStatus.SUCCEEDED
        assert agent.state == AgentState.RESERVED  # still on the live call
        assert len(provider.requests) == 1  # never re-sent

    async def test_worker_crash_after_send_is_not_redispatched(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent, sample_borrower: Borrower,
    ) -> None:
        """A worker died after marking the dispatch SENT (call INITIATED)."""
        call_id = await _allocate(sample_campaign.id)
        past = datetime.now(timezone.utc) - timedelta(seconds=120)
        async with AsyncSessionLocal() as s:
            await s.execute(update(Call).where(Call.id == call_id).values(state=CallState.INITIATED, state_version=1))
            await s.execute(update(DispatchRecord).where(DispatchRecord.call_id == call_id).values(
                status=DispatchStatus.SENT, worker_id="crashed-worker", lease_expires_at=past, attempt_count=1))
            await s.commit()

        provider = StubProvider()
        assert await DispatchWorker(provider).run_once(AsyncSessionLocal) == 0
        assert provider.requests == []  # SENT is never re-claimed

        result = await _reconcile(provider)
        assert result["lost_in_flight_flagged"] == 1
        assert result["ambiguous_resolved"] == 1  # provider has no call for the key
        call, agent, borrower, dispatch = await _snapshot(call_id)
        assert call.state == CallState.FAILED and call.failure_reason == "RECONCILE_NOT_PLACED"
        assert agent.state == AgentState.AVAILABLE
        assert borrower.status == BorrowerStatus.QUEUED
        assert provider.requests == []

    async def test_expired_claim_is_redispatched_exactly_once(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent, sample_borrower: Borrower,
    ) -> None:
        """A worker died after claiming but before sending: safe to re-dispatch."""
        call_id = await _allocate(sample_campaign.id)
        async with AsyncSessionLocal() as s:
            await s.execute(update(DispatchRecord).where(DispatchRecord.call_id == call_id).values(
                status=DispatchStatus.CLAIMED, worker_id="crashed-worker",
                lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=5)))
            await s.commit()

        provider = StubProvider()
        worker = DispatchWorker(provider)
        assert await worker.run_once(AsyncSessionLocal) == 1
        assert await worker.run_once(AsyncSessionLocal) == 0
        call, _, _, dispatch = await _snapshot(call_id)
        assert len(provider.requests) == 1
        assert dispatch.status == DispatchStatus.SUCCEEDED and dispatch.attempt_count == 1
        assert call.state == CallState.INITIATED and call.provider_call_id

    async def test_claims_are_exclusive_and_scoped_to_provider(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent, sample_borrower: Borrower,
    ) -> None:
        await _allocate(sample_campaign.id)
        async with AsyncSessionLocal() as s:
            repo = DispatchRepository(s)
            assert await repo.claim_pending("w-other", provider_id="mock_provider_b") == []
            first = await repo.claim_pending("w1", provider_id="mock_provider_a")
            second = await repo.claim_pending("w2", provider_id="mock_provider_a")
            await s.commit()
        assert len(first) == 1 and second == []

        # A worker whose claim was taken over cannot send.
        async with AsyncSessionLocal() as s:
            assert await DispatchRepository(s).mark_sent(first[0].id, "w2") is False
            assert await DispatchRepository(s).mark_sent(first[0].id, "w1") is True
            await s.commit()

    async def test_repeated_rejections_open_the_circuit_breaker(
        self, db_session: AsyncSession, sample_campaign: Campaign,
    ) -> None:
        from app.services.provider_health import ProviderHealthService

        async with AsyncSessionLocal() as s:
            for i in range(5):
                s.add(Agent(campaign_id=sample_campaign.id, name=f"A{i}", state=AgentState.AVAILABLE))
                s.add(Borrower(campaign_id=sample_campaign.id, synthetic_name=f"B{i}", masked_phone=f"***-{i}", status="QUEUED"))
            await s.commit()

        health = ProviderHealthService("mock_provider_a")
        provider = MockProviderA(simulate_outage=True)
        async with AsyncSessionLocal() as s:
            await CallAllocator().allocate_batch(s, sample_campaign.id, 5)
            await s.commit()
        await DispatchWorker(provider, health_service=health).run_once(AsyncSessionLocal)

        async with AsyncSessionLocal() as s:
            snapshot = await health.get_snapshot(s)
        assert snapshot["circuit_state"] == "OPEN"
        assert snapshot["consecutive_failures"] == 5
