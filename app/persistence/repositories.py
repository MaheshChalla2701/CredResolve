"""Repository pattern — thin data-access layer over ORM models.

Each repository method operates inside a caller-provided session so that
multiple repo operations can participate in a single transaction.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import select, update, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.core.enums import AgentState, BorrowerStatus, CallState, DispatchStatus
from app.persistence.models import (
    Agent,
    AuditEvent,
    Borrower,
    Call,
    Campaign,
    DispatchRecord,
    ProviderEventInbox,
    ProviderHealthRecord,
    SafetyDecisionRecord,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# Campaign repository
# ─────────────────────────────────────────────────────────────────────────────
class CampaignRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def get(self, campaign_id: str) -> Campaign | None:
        result = await self._s.get(Campaign, campaign_id)
        return result

    async def list_active(self) -> Sequence[Campaign]:
        result = await self._s.execute(
            select(Campaign).where(Campaign.status == "ACTIVE")
        )
        return result.scalars().all()

    async def create(self, campaign: Campaign) -> Campaign:
        self._s.add(campaign)
        await self._s.flush()
        return campaign

    async def update_status(self, campaign_id: str, status: str) -> None:
        await self._s.execute(
            update(Campaign)
            .where(Campaign.id == campaign_id)
            .values(status=status, updated_at=_utcnow())
        )


# ─────────────────────────────────────────────────────────────────────────────
# Agent repository
# ─────────────────────────────────────────────────────────────────────────────
class AgentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def get(self, agent_id: str) -> Agent | None:
        return await self._s.get(Agent, agent_id)

    async def get_for_update(self, agent_id: str) -> Agent | None:
        result = await self._s.execute(
            select(Agent).where(Agent.id == agent_id).with_for_update()
        )
        return result.scalar_one_or_none()

    async def count_by_state(self, campaign_id: str) -> dict[str, int]:
        result = await self._s.execute(
            select(Agent.state, func.count(Agent.id))
            .where(Agent.campaign_id == campaign_id)
            .group_by(Agent.state)
        )
        return {row[0]: row[1] for row in result}

    async def list_available(self, campaign_id: str, limit: int = 100) -> Sequence[Agent]:
        result = await self._s.execute(
            select(Agent)
            .where(
                Agent.campaign_id == campaign_id,
                Agent.state == AgentState.AVAILABLE,
                Agent.is_paused.is_(False),
            )
            .limit(limit)
        )
        return result.scalars().all()

    async def claim_available_for_update(
        self, campaign_id: str
    ) -> Agent | None:
        """Select one AVAILABLE agent with a row-level lock, skip locked rows."""
        result = await self._s.execute(
            select(Agent)
            .where(
                Agent.campaign_id == campaign_id,
                Agent.state == AgentState.AVAILABLE,
                Agent.is_paused.is_(False),
            )
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def transition_state(
        self,
        agent_id: str,
        from_state: str,
        to_state: str,
        version: int,
        *,
        current_call_id: str | None = None,
        clear_call: bool = False,
    ) -> bool:
        """Conditional update — only succeeds if state and version match.

        Returns True if the row was updated, False if it was stale.
        """
        values: dict = {
            "state": to_state,
            "version": version + 1,
            "state_changed_at": _utcnow(),
            "updated_at": _utcnow(),
        }
        if current_call_id is not None:
            values["current_call_id"] = current_call_id
        if clear_call:
            values["current_call_id"] = None

        result = await self._s.execute(
            update(Agent)
            .where(
                Agent.id == agent_id,
                Agent.state == from_state,
                Agent.version == version,
            )
            .values(**values)
        )
        return result.rowcount == 1

    async def update_heartbeat(self, agent_id: str) -> None:
        await self._s.execute(
            update(Agent)
            .where(Agent.id == agent_id)
            .values(last_heartbeat=_utcnow(), updated_at=_utcnow())
        )

    async def list_stale_heartbeats(
        self, cutoff: datetime, campaign_id: str | None = None
    ) -> Sequence[Agent]:
        conditions = [
            Agent.last_heartbeat < cutoff,
            Agent.state.in_([
                AgentState.AVAILABLE, AgentState.RESERVED,
                AgentState.DIALING, AgentState.CONNECTED,
            ]),
        ]
        if campaign_id:
            conditions.append(Agent.campaign_id == campaign_id)
        result = await self._s.execute(select(Agent).where(and_(*conditions)))
        return result.scalars().all()

    async def create(self, agent: Agent) -> Agent:
        self._s.add(agent)
        await self._s.flush()
        return agent


# ─────────────────────────────────────────────────────────────────────────────
# Borrower repository
# ─────────────────────────────────────────────────────────────────────────────
class BorrowerRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def get(self, borrower_id: str) -> Borrower | None:
        return await self._s.get(Borrower, borrower_id)

    async def claim_queued_for_update(
        self, campaign_id: str
    ) -> Borrower | None:
        """Claim the highest-priority QUEUED eligible borrower with a row lock."""
        now = _utcnow()
        result = await self._s.execute(
            select(Borrower)
            .where(
                Borrower.campaign_id == campaign_id,
                Borrower.status == BorrowerStatus.QUEUED,
                Borrower.is_dnc.is_(False),
                or_(
                    Borrower.next_eligible_at.is_(None),
                    Borrower.next_eligible_at <= now,
                ),
            )
            .order_by(Borrower.priority.desc(), Borrower.created_at.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def update_status(
        self,
        borrower_id: str,
        status: str,
        *,
        increment_attempts: bool = False,
        next_eligible_at: datetime | None = None,
    ) -> None:
        values: dict = {"status": status, "updated_at": _utcnow()}
        if increment_attempts:
            values["attempt_count"] = Borrower.attempt_count + 1
        if next_eligible_at is not None:
            values["next_eligible_at"] = next_eligible_at
        await self._s.execute(
            update(Borrower).where(Borrower.id == borrower_id).values(**values)
        )

    async def create(self, borrower: Borrower) -> Borrower:
        self._s.add(borrower)
        await self._s.flush()
        return borrower

    async def list_by_campaign(
        self, campaign_id: str, limit: int = 200
    ) -> Sequence[Borrower]:
        result = await self._s.execute(
            select(Borrower)
            .where(Borrower.campaign_id == campaign_id)
            .limit(limit)
        )
        return result.scalars().all()


# ─────────────────────────────────────────────────────────────────────────────
# Call repository
# ─────────────────────────────────────────────────────────────────────────────
class CallRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def get(self, call_id: str) -> Call | None:
        return await self._s.get(Call, call_id)

    async def get_for_update(self, call_id: str) -> Call | None:
        result = await self._s.execute(
            select(Call).where(Call.id == call_id).with_for_update()
        )
        return result.scalar_one_or_none()

    async def get_by_provider_call_id(self, provider_call_id: str) -> Call | None:
        result = await self._s.execute(
            select(Call).where(Call.provider_call_id == provider_call_id)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, key: str) -> Call | None:
        result = await self._s.execute(
            select(Call).where(Call.idempotency_key == key)
        )
        return result.scalar_one_or_none()

    async def transition_state(
        self,
        call_id: str,
        from_state: str,
        to_state: str,
        version: int,
        **extra_values: object,
    ) -> bool:
        """Conditional update on call state + version. Returns True on success."""
        values: dict = {
            "state": to_state,
            "state_version": version + 1,
            "updated_at": _utcnow(),
            **extra_values,
        }
        result = await self._s.execute(
            update(Call)
            .where(
                Call.id == call_id,
                Call.state == from_state,
                Call.state_version == version,
            )
            .values(**values)
        )
        return result.rowcount == 1

    async def count_active(self, campaign_id: str) -> int:
        from app.core.enums import ACTIVE_CALL_STATES
        result = await self._s.execute(
            select(func.count(Call.id)).where(
                Call.campaign_id == campaign_id,
                Call.state.in_(list(ACTIVE_CALL_STATES)),
            )
        )
        return result.scalar_one() or 0

    async def list_active(self, campaign_id: str) -> Sequence[Call]:
        from app.core.enums import ACTIVE_CALL_STATES
        result = await self._s.execute(
            select(Call).where(
                Call.campaign_id == campaign_id,
                Call.state.in_(list(ACTIVE_CALL_STATES)),
            )
        )
        return result.scalars().all()

    async def list_stale_reserved(
        self, cutoff: datetime, campaign_id: str | None = None
    ) -> Sequence[Call]:
        conditions = [
            Call.state == CallState.RESERVED,
            Call.created_at < cutoff,
        ]
        if campaign_id:
            conditions.append(Call.campaign_id == campaign_id)
        result = await self._s.execute(select(Call).where(and_(*conditions)))
        return result.scalars().all()

    async def list_ambiguous(self) -> Sequence[Call]:
        result = await self._s.execute(
            select(Call).where(Call.is_ambiguous.is_(True))
        )
        return result.scalars().all()

    async def create(self, call: Call) -> Call:
        self._s.add(call)
        await self._s.flush()
        return call

    async def list_by_campaign(
        self, campaign_id: str, limit: int = 200
    ) -> Sequence[Call]:
        result = await self._s.execute(
            select(Call)
            .where(Call.campaign_id == campaign_id)
            .order_by(Call.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()


# ─────────────────────────────────────────────────────────────────────────────
# Dispatch repository
# ─────────────────────────────────────────────────────────────────────────────
class DispatchRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def create(self, record: DispatchRecord) -> DispatchRecord:
        self._s.add(record)
        await self._s.flush()
        return record

    async def claim_pending(
        self, worker_id: str, lease_seconds: int = 30, batch_size: int = 10
    ) -> Sequence[DispatchRecord]:
        """Claim pending (or expired-lease) dispatch records atomically."""
        now = _utcnow()
        from datetime import timedelta
        lease_expires = now + timedelta(seconds=lease_seconds)

        result = await self._s.execute(
            select(DispatchRecord)
            .where(
                or_(
                    DispatchRecord.status == DispatchStatus.PENDING,
                    and_(
                        DispatchRecord.status == DispatchStatus.CLAIMED,
                        DispatchRecord.lease_expires_at < now,
                    ),
                )
            )
            .with_for_update(skip_locked=True)
            .limit(batch_size)
        )
        records = list(result.scalars().all())
        for r in records:
            r.status = DispatchStatus.CLAIMED
            r.worker_id = worker_id
            r.lease_expires_at = lease_expires
        await self._s.flush()
        return records

    async def update_result(
        self,
        dispatch_id: str,
        status: str,
        provider_call_id: str | None = None,
        result_code: str | None = None,
        result_detail: dict | None = None,
    ) -> None:
        values: dict = {
            "status": status,
            "updated_at": _utcnow(),
        }
        if provider_call_id is not None:
            values["provider_call_id"] = provider_call_id
        if result_code is not None:
            values["result_code"] = result_code
        if result_detail is not None:
            values["result_detail"] = result_detail

        await self._s.execute(
            update(DispatchRecord)
            .where(DispatchRecord.id == dispatch_id)
            .values(**values)
        )

    async def get_by_call_id(self, call_id: str) -> DispatchRecord | None:
        result = await self._s.execute(
            select(DispatchRecord).where(DispatchRecord.call_id == call_id)
        )
        return result.scalar_one_or_none()


# ─────────────────────────────────────────────────────────────────────────────
# Provider event inbox repository
# ─────────────────────────────────────────────────────────────────────────────
class ProviderEventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def insert_if_new(self, event: ProviderEventInbox) -> tuple[ProviderEventInbox, bool]:
        """Insert event; return (event, is_new). is_new=False means duplicate."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from sqlalchemy import literal

        stmt = (
            pg_insert(ProviderEventInbox)
            .values(
                id=event.id,
                provider_id=event.provider_id,
                provider_event_id=event.provider_event_id,
                provider_call_id=event.provider_call_id,
                event_type=event.event_type,
                payload=event.payload,
                sequence_number=event.sequence_number,
                received_at=event.received_at,
                processing_status="PENDING",
            )
            .on_conflict_do_nothing(constraint="uq_provider_event")
            .returning(ProviderEventInbox.id)
        )
        result = await self._s.execute(stmt)
        inserted_id = result.scalar_one_or_none()
        if inserted_id is None:
            # Duplicate — fetch the existing record
            existing = await self._s.execute(
                select(ProviderEventInbox).where(
                    ProviderEventInbox.provider_id == event.provider_id,
                    ProviderEventInbox.provider_event_id == event.provider_event_id,
                )
            )
            return existing.scalar_one(), False
        await self._s.flush()
        return event, True

    async def mark_processed(
        self, event_id: str, *, error: str | None = None
    ) -> None:
        status = "FAILED" if error else "PROCESSED"
        await self._s.execute(
            update(ProviderEventInbox)
            .where(ProviderEventInbox.id == event_id)
            .values(
                processing_status=status,
                processed_at=_utcnow(),
                processing_error=error,
            )
        )

    async def list_pending(self, limit: int = 50) -> Sequence[ProviderEventInbox]:
        result = await self._s.execute(
            select(ProviderEventInbox)
            .where(ProviderEventInbox.processing_status == "PENDING")
            .order_by(ProviderEventInbox.received_at.asc())
            .limit(limit)
        )
        return result.scalars().all()


# ─────────────────────────────────────────────────────────────────────────────
# Provider health repository
# ─────────────────────────────────────────────────────────────────────────────
class ProviderHealthRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def get_or_create(self, provider_id: str) -> ProviderHealthRecord:
        record = await self._s.get(ProviderHealthRecord, provider_id)
        if record is None:
            record = ProviderHealthRecord(provider_id=provider_id)
            self._s.add(record)
            await self._s.flush()
        return record

    async def get_for_update(self, provider_id: str) -> ProviderHealthRecord:
        result = await self._s.execute(
            select(ProviderHealthRecord)
            .where(ProviderHealthRecord.provider_id == provider_id)
            .with_for_update()
        )
        rec = result.scalar_one_or_none()
        if rec is None:
            rec = ProviderHealthRecord(provider_id=provider_id)
            self._s.add(rec)
            await self._s.flush()
        return rec

    async def save(self, record: ProviderHealthRecord) -> None:
        await self._s.flush()


# ─────────────────────────────────────────────────────────────────────────────
# Safety decision repository
# ─────────────────────────────────────────────────────────────────────────────
class SafetyDecisionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def create(self, record: SafetyDecisionRecord) -> SafetyDecisionRecord:
        self._s.add(record)
        await self._s.flush()
        return record

    async def list_recent(
        self, campaign_id: str, limit: int = 20
    ) -> Sequence[SafetyDecisionRecord]:
        result = await self._s.execute(
            select(SafetyDecisionRecord)
            .where(SafetyDecisionRecord.campaign_id == campaign_id)
            .order_by(SafetyDecisionRecord.decided_at.desc())
            .limit(limit)
        )
        return result.scalars().all()


# ─────────────────────────────────────────────────────────────────────────────
# Audit repository
# ─────────────────────────────────────────────────────────────────────────────
class AuditRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def log(self, event: AuditEvent) -> None:
        self._s.add(event)
        await self._s.flush()
