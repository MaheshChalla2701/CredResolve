"""Unit tests for the Reconciliation Service."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AgentState, CallState, DispatchStatus
from app.persistence.models import Agent, Borrower, Call, Campaign, DispatchRecord
from app.services.reconciliation import ReconciliationService


@pytest.mark.asyncio
class TestReconciliationService:
    async def test_releases_wrap_up_agents_past_timeout(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
    ) -> None:
        agent = Agent(
            id=str(uuid.uuid4()),
            campaign_id=sample_campaign.id,
            name="WrapUpAgent",
            state=AgentState.WRAP_UP,
            current_call_id=None,
            state_changed_at=datetime.now(timezone.utc) - timedelta(seconds=120),
        )
        db_session.add(agent)
        await db_session.commit()

        reconciler = ReconciliationService()
        result = await reconciler.run(db_session)
        await db_session.commit()

        assert result["wrap_up_released"] >= 1
        await db_session.refresh(agent)
        assert agent.state == AgentState.AVAILABLE

    async def test_offline_stale_agent_without_active_call(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
    ) -> None:
        agent = Agent(
            id=str(uuid.uuid4()),
            campaign_id=sample_campaign.id,
            name="StaleAgent",
            state=AgentState.AVAILABLE,
            current_call_id=None,
            last_heartbeat=datetime.now(timezone.utc) - timedelta(seconds=120),
        )
        db_session.add(agent)
        await db_session.commit()

        reconciler = ReconciliationService()
        result = await reconciler.run(db_session)
        await db_session.commit()

        assert result["stale_agents_offline"] >= 1
        await db_session.refresh(agent)
        assert agent.state == AgentState.OFFLINE

    async def test_preserves_reservation_for_agent_with_active_call(
        self,
        db_session: AsyncSession,
        sample_campaign: Campaign,
        sample_borrower: Borrower,
    ) -> None:
        call_id = str(uuid.uuid4())
        agent = Agent(
            id=str(uuid.uuid4()),
            campaign_id=sample_campaign.id,
            name="BusyAgent",
            state=AgentState.CONNECTED,
            current_call_id=call_id,
            last_heartbeat=datetime.now(timezone.utc) - timedelta(seconds=120),
        )
        db_session.add(agent)

        call = Call(
            id=call_id,
            campaign_id=sample_campaign.id,
            agent_id=agent.id,
            borrower_id=sample_borrower.id,
            state=CallState.CONNECTED,
            idempotency_key="reconcile-busy-key",
        )
        db_session.add(call)
        await db_session.commit()

        reconciler = ReconciliationService()
        await reconciler.run(db_session)
        await db_session.commit()

        # Fail closed: must NOT transition agent to OFFLINE while in active call
        await db_session.refresh(agent)
        assert agent.state == AgentState.CONNECTED
