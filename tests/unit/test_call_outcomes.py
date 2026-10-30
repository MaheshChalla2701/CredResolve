"""Regression tests for call outcomes: agent release, borrower retry policy,
duplicate and out-of-order provider events.

Calls are reserved and dispatched through the real CallAllocator and
DispatchWorker, so agents start in the state they really have after dispatch
(RESERVED), not a hand-picked one.
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, BorrowerStatus, CallState
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Call, Campaign, ProviderEventInbox
from app.services.call_allocator import CallAllocator
from app.services.event_processor import EventProcessor
from app.services.reconciliation import ReconciliationService
from app.workers.runtime import DialerRuntime
from conftest import StubProvider, allocate_and_dispatch


async def _state(call_id: str) -> tuple[Call, Agent, Borrower]:
    async with AsyncSessionLocal() as s:
        call = await s.get(Call, call_id)
        agent = await s.get(Agent, call.agent_id)
        borrower = await s.get(Borrower, call.borrower_id)
        return call, agent, borrower


_seq = 0


async def _event(call: Call, event_type: str, event_id: str | None = None) -> bool:
    global _seq
    _seq += 1
    async with AsyncSessionLocal() as s:
        changed = await EventProcessor().process_raw(
            s,
            provider_id="mock_provider_a",
            provider_event_id=event_id or f"{call.provider_call_id}-{event_type}-{_seq}",
            provider_call_id=call.provider_call_id,
            event_type=event_type,
            payload={},
        )
        await s.commit()
    return changed


async def _set_campaign(campaign_id: str, **values) -> None:
    async with AsyncSessionLocal() as s:
        campaign = await s.get(Campaign, campaign_id)
        for k, v in values.items():
            setattr(campaign, k, v)
        await s.commit()


@pytest.mark.asyncio
class TestCallOutcomes:
    async def test_dispatched_call_holds_agent_reserved(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        sample_borrower: Borrower, stub_provider: StubProvider,
    ) -> None:
        [call_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        call, agent, borrower = await _state(call_id)
        assert call.state == CallState.INITIATED and call.provider_call_id
        assert agent.state == AgentState.RESERVED and agent.current_call_id == call_id
        assert borrower.status == BorrowerStatus.ALLOCATED and borrower.attempt_count == 1

    async def test_terminal_event_before_ringing_releases_agent(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        sample_borrower: Borrower, stub_provider: StubProvider,
    ) -> None:
        """Previously the agent stayed RESERVED forever (RESERVED→WRAP_UP was skipped)."""
        [call_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        call, _, _ = await _state(call_id)

        assert await _event(call, "busy") is True
        call, agent, borrower = await _state(call_id)
        assert call.state == CallState.FAILED and call.failure_reason == "busy"
        assert agent.state == AgentState.AVAILABLE and agent.current_call_id is None
        assert borrower.status == BorrowerStatus.QUEUED and borrower.next_eligible_at is not None

        # The delayed 'ringing' arrives afterwards: ignored, nothing regresses.
        assert await _event(call, "ringing") is False
        call, agent, _ = await _state(call_id)
        assert call.state == CallState.FAILED and agent.state == AgentState.AVAILABLE

    async def test_no_answer_is_retryable_and_agent_is_reused(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        sample_borrower: Borrower, stub_provider: StubProvider,
    ) -> None:
        await _set_campaign(sample_campaign.id, retry_cooldown_seconds=0)
        [call_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        call, _, _ = await _state(call_id)
        await _event(call, "ringing")
        await _event(call, "no_answer")

        call, agent, borrower = await _state(call_id)
        assert call.state == CallState.FAILED and call.failure_reason == "no_answer"
        assert agent.state == AgentState.AVAILABLE  # straight back, no wrap-up for an unanswered call
        assert borrower.status == BorrowerStatus.QUEUED  # previously wrongly COMPLETED

        # The same agent and borrower are eligible again.
        [retry_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        retry, agent, borrower = await _state(retry_id)
        assert retry.agent_id == sample_agent.id and retry.borrower_id == sample_borrower.id
        assert retry.attempt_number == 2 and borrower.attempt_count == 2

    async def test_retry_limit_exhausts_borrower(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        sample_borrower: Borrower, stub_provider: StubProvider,
    ) -> None:
        await _set_campaign(sample_campaign.id, retry_cooldown_seconds=0, max_attempts_per_borrower=2)
        for expected in (BorrowerStatus.QUEUED, BorrowerStatus.EXHAUSTED):
            [call_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
            call, _, _ = await _state(call_id)
            await _event(call, "failed")
            _, agent, borrower = await _state(call_id)
            assert borrower.status == expected
            assert agent.state == AgentState.AVAILABLE
        async with AsyncSessionLocal() as s:
            assert await CallAllocator().allocate_batch(s, sample_campaign.id, 1) == []

    async def test_completed_call_wraps_up_then_reconciliation_frees_agent(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        sample_borrower: Borrower, stub_provider: StubProvider,
    ) -> None:
        await _set_campaign(sample_campaign.id, wrap_up_seconds=0)
        [call_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        call, _, _ = await _state(call_id)
        for event_type in ("ringing", "answered", "connected", "completed"):
            assert await _event(call, event_type) is True

        call, agent, borrower = await _state(call_id)
        assert call.state == CallState.COMPLETED
        assert agent.state == AgentState.WRAP_UP and agent.current_call_id is None
        assert borrower.status == BorrowerStatus.COMPLETED

        async with AsyncSessionLocal() as s:
            result = await ReconciliationService().run(s)
            await s.commit()
        assert result["wrap_up_released"] == 1 and result["errors"] == []
        _, agent, _ = await _state(call_id)
        assert agent.state == AgentState.AVAILABLE

    async def test_connected_before_answered_fast_forwards(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        sample_borrower: Borrower, stub_provider: StubProvider,
    ) -> None:
        [call_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        call, _, _ = await _state(call_id)
        await _event(call, "ringing")
        assert await _event(call, "connected") is True  # previously dropped as invalid

        call, agent, _ = await _state(call_id)
        assert call.state == CallState.CONNECTED
        assert call.answered_at is not None and call.connected_at is not None
        assert agent.state == AgentState.CONNECTED

        assert await _event(call, "answered") is False  # late, already implied
        call, _, _ = await _state(call_id)
        assert call.state == CallState.CONNECTED

    async def test_late_connected_after_completed_backfills_fact(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        sample_borrower: Borrower, stub_provider: StubProvider,
    ) -> None:
        [call_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        call, _, _ = await _state(call_id)
        await _event(call, "answered")
        await _event(call, "completed")
        assert await _event(call, "connected") is False

        call, agent, borrower = await _state(call_id)
        assert call.state == CallState.COMPLETED
        assert call.connected_at is not None  # the fact is kept, the state is not regressed
        assert agent.state == AgentState.WRAP_UP
        assert borrower.status == BorrowerStatus.COMPLETED

    async def test_duplicate_terminal_event_settles_once(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        sample_borrower: Borrower, stub_provider: StubProvider,
    ) -> None:
        [call_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        call, _, _ = await _state(call_id)
        assert await _event(call, "busy", event_id="busy-1") is True
        assert await _event(call, "busy", event_id="busy-1") is False

        call, agent, borrower = await _state(call_id)
        assert call.state == CallState.FAILED and call.state_version == 2
        assert borrower.attempt_count == 1 and borrower.status == BorrowerStatus.QUEUED
        async with AsyncSessionLocal() as s:
            rows = (await s.execute(
                select(ProviderEventInbox).where(ProviderEventInbox.provider_event_id == "busy-1")
            )).scalars().all()
        assert len(rows) == 1

    async def test_late_event_does_not_touch_agent_on_a_newer_call(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        stub_provider: StubProvider,
    ) -> None:
        await _set_campaign(sample_campaign.id, retry_cooldown_seconds=0)
        async with AsyncSessionLocal() as s:
            for i in range(2):
                s.add(Borrower(id=str(uuid.uuid4()), campaign_id=sample_campaign.id,
                               synthetic_name=f"B{i}", masked_phone=f"***-{i}", status="QUEUED",
                               priority=10 - i))
            await s.commit()
        [first_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        first, _, _ = await _state(first_id)
        await _event(first, "busy")
        [second_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        _, agent, _ = await _state(second_id)
        assert agent.state == AgentState.RESERVED and agent.current_call_id == second_id

        # A stray late event for the first (finished) call must not free the agent.
        assert await _event(first, "no_answer") is False
        _, agent, _ = await _state(second_id)
        assert agent.state == AgentState.RESERVED and agent.current_call_id == second_id

    async def test_event_arriving_before_provider_call_id_is_retried(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent,
        sample_borrower: Borrower, stub_provider: StubProvider,
    ) -> None:
        """The provider can emit events before the worker records its call id."""
        provider_call_id = "stub-1-early0"
        async with AsyncSessionLocal() as s:
            changed = await EventProcessor().process_raw(
                s, provider_id="mock_provider_a", provider_event_id="early-ring",
                provider_call_id=provider_call_id, event_type="ringing", payload={},
            )
            await s.commit()
        assert changed is False
        async with AsyncSessionLocal() as s:
            inbox = (await s.execute(
                select(ProviderEventInbox).where(ProviderEventInbox.provider_event_id == "early-ring")
            )).scalar_one()
        assert inbox.processing_status == "PENDING"
        assert inbox.processing_error == "AWAITING_CALL_MATCH"

        [call_id] = await allocate_and_dispatch(sample_campaign.id, stub_provider)
        async with AsyncSessionLocal() as s:
            await s.execute(Call.__table__.update().where(Call.id == call_id).values(provider_call_id=provider_call_id))
            await s.commit()

        runtime = DialerRuntime({"mock_provider_a": stub_provider})
        assert await runtime.retry_unmatched_events() == 1
        call, agent, _ = await _state(call_id)
        assert call.state == CallState.RINGING
        assert agent.state == AgentState.DIALING
