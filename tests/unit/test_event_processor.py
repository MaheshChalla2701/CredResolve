"""Unit tests for the Idempotent Event Processor."""
from __future__ import annotations

import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, BorrowerStatus, CallState, ProviderEventType
from app.persistence.models import Agent, Borrower, Call, Campaign
from app.services.event_processor import EventProcessor


@pytest.mark.asyncio
class TestEventProcessor:
    async def test_processes_valid_call_lifecycle(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        # Create an initiated call
        provider_call_id = "prov-call-100"
        call = Call(
            id=str(uuid.uuid4()),
            campaign_id=sample_campaign.id,
            agent_id=sample_agent.id,
            borrower_id=sample_borrower.id,
            provider_id="mock_provider_a",
            provider_call_id=provider_call_id,
            state=CallState.INITIATED,
            idempotency_key="idemp-100",
        )
        db_session.add(call)
        sample_agent.state = AgentState.DIALING
        sample_agent.current_call_id = call.id
        await db_session.commit()

        processor = EventProcessor()

        # 1. RINGING event
        changed = await processor.process_raw(
            db_session,
            provider_id="mock_provider_a",
            provider_event_id="evt-ring-1",
            provider_call_id=provider_call_id,
            event_type=ProviderEventType.RINGING,
            payload={},
        )
        assert changed is True
        await db_session.refresh(call)
        assert call.state == CallState.RINGING

        # 2. ANSWERED event
        changed = await processor.process_raw(
            db_session,
            provider_id="mock_provider_a",
            provider_event_id="evt-ans-1",
            provider_call_id=provider_call_id,
            event_type=ProviderEventType.ANSWERED,
            payload={},
        )
        assert changed is True
        await db_session.refresh(call)
        assert call.state == CallState.ANSWERED

        # 3. CONNECTED event
        changed = await processor.process_raw(
            db_session,
            provider_id="mock_provider_a",
            provider_event_id="evt-conn-1",
            provider_call_id=provider_call_id,
            event_type=ProviderEventType.CONNECTED,
            payload={},
        )
        assert changed is True
        await db_session.refresh(call)
        assert call.state == CallState.CONNECTED
        await db_session.refresh(sample_agent)
        assert sample_agent.state == AgentState.CONNECTED

        # 4. COMPLETED event
        changed = await processor.process_raw(
            db_session,
            provider_id="mock_provider_a",
            provider_event_id="evt-comp-1",
            provider_call_id=provider_call_id,
            event_type=ProviderEventType.COMPLETED,
            payload={"duration_seconds": 45.0},
        )
        assert changed is True
        await db_session.refresh(call)
        assert call.state == CallState.COMPLETED
        await db_session.refresh(sample_agent)
        assert sample_agent.state == AgentState.WRAP_UP

    async def test_duplicate_event_is_suppressed(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        provider_call_id = "prov-call-dup"
        call = Call(
            id=str(uuid.uuid4()),
            campaign_id=sample_campaign.id,
            agent_id=sample_agent.id,
            borrower_id=sample_borrower.id,
            provider_id="mock_provider_a",
            provider_call_id=provider_call_id,
            state=CallState.INITIATED,
            idempotency_key="idemp-dup",
        )
        db_session.add(call)
        await db_session.commit()

        processor = EventProcessor()
        # First delivery
        r1 = await processor.process_raw(
            db_session,
            provider_id="mock_provider_a",
            provider_event_id="same-event-id",
            provider_call_id=provider_call_id,
            event_type=ProviderEventType.RINGING,
            payload={},
        )
        assert r1 is True

        # Second delivery with identical event_id
        r2 = await processor.process_raw(
            db_session,
            provider_id="mock_provider_a",
            provider_event_id="same-event-id",
            provider_call_id=provider_call_id,
            event_type=ProviderEventType.RINGING,
            payload={},
        )
        assert r2 is False

    async def test_out_of_order_event_dropped_on_terminal_call(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        provider_call_id = "prov-call-terminal"
        call = Call(
            id=str(uuid.uuid4()),
            campaign_id=sample_campaign.id,
            agent_id=sample_agent.id,
            borrower_id=sample_borrower.id,
            provider_id="mock_provider_a",
            provider_call_id=provider_call_id,
            state=CallState.COMPLETED,  # already finished
            idempotency_key="idemp-term",
        )
        db_session.add(call)
        await db_session.commit()

        processor = EventProcessor()
        # Late ANSWERED event arriving after call is already COMPLETED
        changed = await processor.process_raw(
            db_session,
            provider_id="mock_provider_a",
            provider_event_id="late-ans-evt",
            provider_call_id=provider_call_id,
            event_type=ProviderEventType.ANSWERED,
            payload={},
        )
        # Must be safely dropped without error
        assert changed is False
        await db_session.refresh(call)
        assert call.state == CallState.COMPLETED
