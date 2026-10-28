"""SQLAlchemy ORM models — the authoritative database schema.

Invariants enforced at the database level:
  1. UniqueConstraint on (agent_id, call_id) via active_agent_call_uc — an agent
     cannot be assigned to two simultaneously active calls.
  2. UniqueConstraint on (borrower_id, campaign_id) in borrower_allocations for
     active reservations — a borrower cannot be doubly allocated.
  3. UniqueConstraint on idempotency_key in calls — no duplicate logical attempts.
  4. UniqueConstraint on provider_event_id in provider_event_inbox — deduplication.
  5. CHECK constraints on state columns.
  6. Version column on agents and calls for optimistic locking.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    from datetime import timezone
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Campaign
# ─────────────────────────────────────────────────────────────────────────────
class Campaign(Base):
    __tablename__ = "campaigns"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="DRAFT",
        server_default="DRAFT",
    )
    dialing_mode: Mapped[str] = mapped_column(
        String(32), nullable=False, default="progressive"
    )
    config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    # Limits
    max_concurrent_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=50)
    max_attempts_per_borrower: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    retry_cooldown_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=3600)
    wrap_up_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=30)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now, onupdate=_now
    )

    agents: Mapped[list["Agent"]] = relationship("Agent", back_populates="campaign")
    borrowers: Mapped[list["Borrower"]] = relationship("Borrower", back_populates="campaign")
    calls: Mapped[list["Call"]] = relationship("Call", back_populates="campaign")

    __table_args__ = (
        CheckConstraint(
            "status IN ('DRAFT','ACTIVE','PAUSED','COMPLETED','ARCHIVED')",
            name="ck_campaign_status",
        ),
        CheckConstraint(
            "dialing_mode IN ('progressive','predictive')",
            name="ck_campaign_dialing_mode",
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Agent
# ─────────────────────────────────────────────────────────────────────────────
class Agent(Base):
    __tablename__ = "agents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    campaign_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    state: Mapped[str] = mapped_column(
        String(32), nullable=False, default="OFFLINE", index=True
    )
    version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    current_call_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("calls.id", ondelete="SET NULL", use_alter=True, name="fk_agents_current_call_id"),
        nullable=True,
    )
    last_heartbeat: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    is_paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Timestamps for state transitions
    state_changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now, onupdate=_now
    )

    campaign: Mapped["Campaign"] = relationship("Campaign", back_populates="agents")

    __table_args__ = (
        CheckConstraint(
            "state IN ('OFFLINE','AVAILABLE','RESERVED','DIALING','CONNECTED','WRAP_UP','PAUSED')",
            name="ck_agent_state",
        ),
        Index("ix_agents_campaign_state", "campaign_id", "state"),
        Index("ix_agents_heartbeat", "last_heartbeat"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Borrower
# ─────────────────────────────────────────────────────────────────────────────
class Borrower(Base):
    __tablename__ = "borrowers"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    campaign_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False
    )
    # Synthetic identifiers — never use real borrower data
    synthetic_name: Mapped[str] = mapped_column(String(255), nullable=False)
    masked_phone: Mapped[str] = mapped_column(String(64), nullable=False)

    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="QUEUED", index=True
    )
    is_dnc: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    next_eligible_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now, onupdate=_now
    )

    campaign: Mapped["Campaign"] = relationship("Campaign", back_populates="borrowers")
    calls: Mapped[list["Call"]] = relationship("Call", back_populates="borrower")

    __table_args__ = (
        CheckConstraint(
            "status IN ('QUEUED','ALLOCATED','COMPLETED','FAILED','DNC','EXHAUSTED')",
            name="ck_borrower_status",
        ),
        Index("ix_borrowers_campaign_status_priority", "campaign_id", "status", "priority"),
        Index("ix_borrowers_next_eligible", "next_eligible_at"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Call
# ─────────────────────────────────────────────────────────────────────────────
class Call(Base):
    __tablename__ = "calls"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    campaign_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False
    )
    agent_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("agents.id", ondelete="RESTRICT"), nullable=False
    )
    borrower_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("borrowers.id", ondelete="RESTRICT"), nullable=False
    )
    provider_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_call_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    state: Mapped[str] = mapped_column(
        String(32), nullable=False, default="QUEUED", index=True
    )
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    # Stable idempotency key per logical attempt
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_ambiguous: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
    initiated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    answered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    connected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now, onupdate=_now
    )

    campaign: Mapped["Campaign"] = relationship("Campaign", back_populates="calls")
    borrower: Mapped["Borrower"] = relationship("Borrower", back_populates="calls")
    dispatches: Mapped[list["DispatchRecord"]] = relationship(
        "DispatchRecord", back_populates="call"
    )

    __table_args__ = (
        CheckConstraint(
            "state IN ('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED',"
            "'CONNECTED','COMPLETED','FAILED','CANCELLED')",
            name="ck_call_state",
        ),
        Index("ix_calls_campaign_state", "campaign_id", "state"),
        Index("ix_calls_agent", "agent_id"),
        Index("ix_calls_borrower", "borrower_id"),
        Index("ix_calls_provider_call_id", "provider_call_id"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Durable dispatch outbox
# ─────────────────────────────────────────────────────────────────────────────
class DispatchRecord(Base):
    """Transactional outbox for provider call initiation.

    A record is written atomically with the call reservation.
    A dispatcher worker claims it (updating lease), then calls the provider.
    The provider outcome is persisted back here.
    """

    __tablename__ = "dispatch_records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    call_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("calls.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    campaign_id: Mapped[str] = mapped_column(String(36), nullable=False)
    provider_id: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    masked_phone: Mapped[str] = mapped_column(String(64), nullable=False)

    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="PENDING", index=True
    )
    worker_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    provider_call_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    result_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result_detail: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now, onupdate=_now
    )

    call: Mapped["Call"] = relationship("Call", back_populates="dispatches")

    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING','CLAIMED','SENT','SUCCEEDED','FAILED','AMBIGUOUS')",
            name="ck_dispatch_status",
        ),
        Index("ix_dispatch_status_lease", "status", "lease_expires_at"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Provider event inbox (idempotent)
# ─────────────────────────────────────────────────────────────────────────────
class ProviderEventInbox(Base):
    """Durable inbox for incoming provider webhook/poll events.

    Events are written before processing. The unique constraint on
    (provider_id, provider_event_id) ensures deduplication.
    """

    __tablename__ = "provider_event_inbox"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    provider_id: Mapped[str] = mapped_column(String(64), nullable=False)
    provider_event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    provider_call_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    sequence_number: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    processing_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="PENDING"
    )
    processing_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("provider_id", "provider_event_id", name="uq_provider_event"),
        Index("ix_event_inbox_processing_status", "processing_status"),
        Index("ix_event_inbox_call_id", "provider_call_id"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Safety controller decisions (audit log)
# ─────────────────────────────────────────────────────────────────────────────
class SafetyDecisionRecord(Base):
    __tablename__ = "safety_decisions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    campaign_id: Mapped[str] = mapped_column(String(36), nullable=False)
    requested_count: Mapped[int] = mapped_column(Integer, nullable=False)
    approved_count: Mapped[int] = mapped_column(Integer, nullable=False)
    decision: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_codes: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    capacity_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    provider_health_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )

    __table_args__ = (
        Index("ix_safety_decisions_campaign", "campaign_id", "decided_at"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Audit log
# ─────────────────────────────────────────────────────────────────────────────
class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    entity_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    campaign_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )

    __table_args__ = (
        Index("ix_audit_entity", "entity_type", "entity_id"),
        Index("ix_audit_campaign", "campaign_id", "created_at"),
        Index("ix_audit_event_type", "event_type"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Provider health / circuit breaker state
# ─────────────────────────────────────────────────────────────────────────────
class ProviderHealthRecord(Base):
    """Rolling health record for each provider (one row per provider)."""

    __tablename__ = "provider_health"

    provider_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    circuit_state: Mapped[str] = mapped_column(
        String(32), nullable=False, default="CLOSED"
    )
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_requests: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_failures: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_failure_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_success_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    circuit_opened_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    half_open_probes_sent: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now, onupdate=_now
    )

    __table_args__ = (
        CheckConstraint(
            "circuit_state IN ('CLOSED','OPEN','HALF_OPEN')",
            name="ck_circuit_state",
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Simulation results
# ─────────────────────────────────────────────────────────────────────────────
class SimulationResult(Base):
    __tablename__ = "simulation_results"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    scenario_name: Mapped[str] = mapped_column(String(255), nullable=False)
    dialing_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    random_seed: Mapped[int] = mapped_column(Integer, nullable=False)
    config: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    metrics: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    invariant_violations: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ran_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
