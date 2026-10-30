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

    async def lock_for_dialing(self, campaign_id: str) -> Campaign | None:
        """Serialize call allocation for one campaign until this transaction ends.

        A write to the campaign row: on PostgreSQL it takes the row lock, so a
        concurrent dialing transaction for the same campaign waits here; on
        SQLite it takes the database write lock. Reads made after this (active
        call count, agent capacity) therefore cannot be invalidated by another
        allocation before commit. Returns the fresh campaign, or None if it is
        missing or not ACTIVE (no lock is held then — nothing may be allocated).
        """
        result = await self._s.execute(
            update(Campaign)
            .where(Campaign.id == campaign_id, Campaign.status == "ACTIVE")
            .values(updated_at=_utcnow())
        )
        if result.rowcount != 1:
            return None
        fresh = await self._s.execute(
            select(Campaign)
            .where(Campaign.id == campaign_id)
            .execution_options(populate_existing=True)
        )
        return fresh.scalar_one()


# ─────────────────────────────────────────────────────────────────────────────
# Agent repository
# ─────────────────────────────────────────────────────────────────────────────
class AgentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def get(self, agent_id: str) -> Agent | None:
        return await self._s.get(Agent, agent_id)

    async def get_for_update(self, agent_id: str) -> Agent | None:
        # populate_existing: refresh an already-loaded instance with the row we
        # just locked, otherwise the session would hand back stale attributes.
        result = await self._s.execute(
            select(Agent)
            .where(Agent.id == agent_id)
            .with_for_update()
            .execution_options(populate_existing=True)
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

    @staticmethod
    def _eligible(campaign_id: str) -> list:
        now = _utcnow()
        return [
            Borrower.campaign_id == campaign_id,
            Borrower.status == BorrowerStatus.QUEUED,
            Borrower.is_dnc.is_(False),
            or_(
                Borrower.next_eligible_at.is_(None),
                Borrower.next_eligible_at <= now,
            ),
        ]

    async def claim_queued_for_update(
        self, campaign_id: str
    ) -> Borrower | None:
        """Claim the highest-priority QUEUED eligible borrower with a row lock."""
        result = await self._s.execute(
            select(Borrower)
            .where(*self._eligible(campaign_id))
            .order_by(Borrower.priority.desc(), Borrower.created_at.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def count_eligible(self, campaign_id: str) -> int:
        """Borrowers claim_queued_for_update() could hand out right now."""
        result = await self._s.execute(
            select(func.count(Borrower.id)).where(*self._eligible(campaign_id))
        )
        return result.scalar_one() or 0

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

    async def mark_allocated(self, borrower_id: str) -> bool:
        """Conditional QUEUED -> ALLOCATED (+1 attempt). False if no longer QUEUED."""
        result = await self._s.execute(
            update(Borrower)
            .where(Borrower.id == borrower_id, Borrower.status == BorrowerStatus.QUEUED)
            .values(
                status=BorrowerStatus.ALLOCATED,
                attempt_count=Borrower.attempt_count + 1,
                updated_at=_utcnow(),
            )
        )
        return result.rowcount == 1

    async def undo_allocation(self, borrower_id: str) -> None:
        """Compensate a mark_allocated() whose surrounding allocation failed."""
        await self._s.execute(
            update(Borrower)
            .where(Borrower.id == borrower_id, Borrower.status == BorrowerStatus.ALLOCATED)
            .values(
                status=BorrowerStatus.QUEUED,
                attempt_count=Borrower.attempt_count - 1,
                updated_at=_utcnow(),
            )
        )

    async def settle_allocation(
        self, borrower_id: str, status: str, *, next_eligible_at: datetime | None = None
    ) -> bool:
        """Conditional ALLOCATED -> status. False if the borrower was already settled."""
        values: dict = {"status": status, "updated_at": _utcnow()}
        if next_eligible_at is not None:
            values["next_eligible_at"] = next_eligible_at
        result = await self._s.execute(
            update(Borrower)
            .where(Borrower.id == borrower_id, Borrower.status == BorrowerStatus.ALLOCATED)
            .values(**values)
        )
        return result.rowcount == 1

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
            select(Call)
            .where(Call.id == call_id)
            .with_for_update()
            .execution_options(populate_existing=True)
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

    async def list_stale_in_progress(self, cutoff: datetime) -> Sequence[Call]:
        """Provider-side calls (INITIATED..CONNECTED) with no state change since cutoff."""
        result = await self._s.execute(
            select(Call).where(
                Call.state.in_([
                    CallState.INITIATED, CallState.RINGING,
                    CallState.ANSWERED, CallState.CONNECTED,
                ]),
                Call.is_ambiguous.is_(False),
                Call.updated_at < cutoff,
            )
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
        self,
        worker_id: str,
        lease_seconds: int = 30,
        batch_size: int = 10,
        *,
        provider_id: str | None = None,
    ) -> Sequence[DispatchRecord]:
        """Claim PENDING (or expired-lease CLAIMED) dispatch records, oldest first.

        Candidates are selected with FOR UPDATE SKIP LOCKED (PostgreSQL), then each
        one is claimed with a conditional UPDATE, so the claim is a compare-and-set
        on every backend (SQLite ignores FOR UPDATE).

        Only CLAIMED records are reclaimable. The worker commits call INITIATED and
        dispatch SENT *before* contacting the provider, so a record still CLAIMED
        proves the provider was never called and re-dispatching cannot duplicate it.
        """
        from datetime import timedelta

        now = _utcnow()
        lease_expires = now + timedelta(seconds=lease_seconds)
        claimable = or_(
            DispatchRecord.status == DispatchStatus.PENDING,
            and_(
                DispatchRecord.status == DispatchStatus.CLAIMED,
                DispatchRecord.lease_expires_at < now,
            ),
        )
        conditions = [claimable]
        if provider_id is not None:
            conditions.append(DispatchRecord.provider_id == provider_id)

        result = await self._s.execute(
            select(DispatchRecord.id)
            .where(*conditions)
            .order_by(DispatchRecord.created_at)
            .with_for_update(skip_locked=True)
            .limit(batch_size)
        )
        claimed_ids = []
        for record_id in result.scalars().all():
            claimed = await self._s.execute(
                update(DispatchRecord)
                .where(DispatchRecord.id == record_id, claimable)
                .values(
                    status=DispatchStatus.CLAIMED,
                    worker_id=worker_id,
                    lease_expires_at=lease_expires,
                    updated_at=now,
                )
            )
            if claimed.rowcount == 1:
                claimed_ids.append(record_id)

        if not claimed_ids:
            return []
        records = await self._s.execute(
            select(DispatchRecord)
            .where(DispatchRecord.id.in_(claimed_ids))
            .order_by(DispatchRecord.created_at)
            .execution_options(populate_existing=True)
        )
        return list(records.scalars().all())

    async def mark_sent(self, dispatch_id: str, worker_id: str) -> bool:
        """Fence: CLAIMED-by-me -> SENT. False if the claim was lost to another worker."""
        result = await self._s.execute(
            update(DispatchRecord)
            .where(
                DispatchRecord.id == dispatch_id,
                DispatchRecord.status == DispatchStatus.CLAIMED,
                DispatchRecord.worker_id == worker_id,
            )
            .values(
                status=DispatchStatus.SENT,
                attempt_count=DispatchRecord.attempt_count + 1,
                updated_at=_utcnow(),
            )
        )
        return result.rowcount == 1

    async def list_lost_in_flight(self, cutoff: datetime) -> Sequence[DispatchRecord]:
        """SENT records whose lease expired without a recorded provider outcome."""
        result = await self._s.execute(
            select(DispatchRecord).where(
                DispatchRecord.status == DispatchStatus.SENT,
                DispatchRecord.lease_expires_at < cutoff,
            )
        )
        return result.scalars().all()

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

    async def get_by_call_id_for_update(self, call_id: str) -> DispatchRecord | None:
        """Lock the call's dispatch record (take the call's lock first — see
        app/persistence/retry.py for the lock order)."""
        result = await self._s.execute(
            select(DispatchRecord)
            .where(DispatchRecord.call_id == call_id)
            .with_for_update()
            .execution_options(populate_existing=True)
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

    async def note_unmatched(self, event_id: str) -> None:
        """Leave the event PENDING (to be retried) but record why it is waiting."""
        await self._s.execute(
            update(ProviderEventInbox)
            .where(ProviderEventInbox.id == event_id)
            .values(processing_error="AWAITING_CALL_MATCH")
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

    async def _ensure_row(self, provider_id: str) -> None:
        """Create the provider's row if missing. INSERT ... ON CONFLICT DO NOTHING,
        so two transactions seeing a new provider at the same time both succeed
        (a plain INSERT made the second one fail on the primary key)."""
        if self._s.get_bind().dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        await self._s.execute(
            insert(ProviderHealthRecord)
            .values(provider_id=provider_id)
            .on_conflict_do_nothing(index_elements=["provider_id"])
        )

    async def get_or_create(self, provider_id: str) -> ProviderHealthRecord:
        record = await self._s.get(ProviderHealthRecord, provider_id)
        if record is None:
            await self._ensure_row(provider_id)
            record = await self._s.get(ProviderHealthRecord, provider_id)
        return record

    async def get_for_update(self, provider_id: str) -> ProviderHealthRecord:
        query = (
            select(ProviderHealthRecord)
            .where(ProviderHealthRecord.provider_id == provider_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        rec = (await self._s.execute(query)).scalar_one_or_none()
        if rec is None:
            await self._ensure_row(provider_id)
            rec = (await self._s.execute(query)).scalar_one()
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
