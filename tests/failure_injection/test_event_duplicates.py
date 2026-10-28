"""Failure injection tests: concurrent duplicate webhooks and out-of-order events."""
from __future__ import annotations

import asyncio
import uuid
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import CallState, ProviderEventType
from app.persistence.database import AsyncSessionLocal
from app.persistence.models import Agent, Borrower, Call, Campaign
from app.services.event_processor import EventProcessor


@pytest.mark.asyncio
class TestEventDuplicatesAndOutOfOrder:
    async def test_concurrent_duplicate_events_only_one_processed(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        call = Call(
            id=str(uuid.uuid4()),
            campaign_id=sample_campaign.id,
            agent_id=sample_agent.id,
            borrower_id=sample_borrower.id,
            provider_id="mock_provider_a",
            provider_call_id="call-dup-race",
            state=CallState.INITIATED,
            idempotency_key="idemp-dup-race",
        )
        db_session.add(call)
        await db_session.commit()

        processor = EventProcessor()
        results: list[bool] = []

        async def send_event() -> None:
            for _ in range(10):
                try:
                    async with AsyncSessionLocal() as session:
                        res = await processor.process_raw(
                            session,
                            provider_id="mock_provider_a",
                            provider_event_id="same-evt-id-race",
                            provider_call_id="call-dup-race",
                            event_type=ProviderEventType.RINGING,
                            payload={},
                        )
                        await session.commit()
                        results.append(res)
                        break
                except Exception:
                    await asyncio.sleep(0.05)

        # 5 simultaneous webhook calls with exact same event ID
        tasks = [asyncio.create_task(send_event()) for _ in range(5)]
        await asyncio.gather(*tasks)

        # Exactly ONE should succeed and return True; other 4 should return False (deduplicated)
        assert results.count(True) == 1
        assert results.count(False) == 4

    async def test_event_out_of_order_skipped_without_corrupting_state(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_agent: Agent,
        sample_borrower: Borrower,
    ) -> None:
        call = Call(
            id=str(uuid.uuid4()),
            campaign_id=sample_campaign.id,
            agent_id=sample_agent.id,
            borrower_id=sample_borrower.id,
            provider_id="mock_provider_a",
            provider_call_id="call-ooo-test",
            state=CallState.INITIATED,
            idempotency_key="idemp-ooo-test",
        )
        db_session.add(call)
        await db_session.commit()

        processor = EventProcessor()

        # Step 1: Arrive directly with COMPLETED (provider skipped intermediate events)
        res1 = await processor.process_raw(
            db_session,
            provider_id="mock_provider_a",
            provider_event_id="evt-comp-first",
            provider_call_id="call-ooo-test",
            event_type=ProviderEventType.COMPLETED,
            payload={},
        )
        assert res1 is True
        await db_session.refresh(call)
        assert call.state == CallState.COMPLETED

        # Step 2: Now delayed RINGING arrives
        res2 = await processor.process_raw(
            db_session,
            provider_id="mock_provider_a",
            provider_event_id="evt-ring-delayed",
            provider_call_id="call-ooo-test",
            event_type=ProviderEventType.RINGING,
            payload={},
        )
        # Must be discarded without modifying call state
        assert res2 is False
        await db_session.refresh(call)
        assert call.state == CallState.COMPLETED
