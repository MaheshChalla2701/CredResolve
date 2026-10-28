"""Initial schema — campaigns, agents, borrowers, calls, outbox, inbox, audit, safety, health.

Revision ID: 001_initial
Revises:
Create Date: 2026-10-27 15:30:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── campaigns ──
    op.create_table(
        "campaigns",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="DRAFT"),
        sa.Column("dialing_mode", sa.String(32), nullable=False, server_default="progressive"),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("max_concurrent_calls", sa.Integer(), nullable=False, server_default="50"),
        sa.Column("max_attempts_per_borrower", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("retry_cooldown_seconds", sa.Integer(), nullable=False, server_default="3600"),
        sa.Column("wrap_up_seconds", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('DRAFT','ACTIVE','PAUSED','COMPLETED','ARCHIVED')",
            name="ck_campaign_status",
        ),
        sa.CheckConstraint(
            "dialing_mode IN ('progressive','predictive')",
            name="ck_campaign_dialing_mode",
        ),
    )

    # ── calls (table definition before agents circular FK) ──
    op.create_table(
        "calls",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("campaign_id", sa.String(36), sa.ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False),
        sa.Column("agent_id", sa.String(36), nullable=False),
        sa.Column("borrower_id", sa.String(36), nullable=False),
        sa.Column("provider_id", sa.String(64), nullable=True),
        sa.Column("provider_call_id", sa.String(255), nullable=True),
        sa.Column("state", sa.String(32), nullable=False, server_default="QUEUED"),
        sa.Column("state_version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("idempotency_key", sa.String(255), nullable=False, unique=True),
        sa.Column("attempt_number", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("is_ambiguous", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("initiated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("answered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("connected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "state IN ('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED','COMPLETED','FAILED','CANCELLED')",
            name="ck_call_state",
        ),
    )
    op.create_index("ix_calls_campaign_state", "calls", ["campaign_id", "state"])
    op.create_index("ix_calls_agent", "calls", ["agent_id"])
    op.create_index("ix_calls_borrower", "calls", ["borrower_id"])
    op.create_index("ix_calls_provider_call_id", "calls", ["provider_call_id"])

    # ── agents ──
    op.create_table(
        "agents",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("campaign_id", sa.String(36), sa.ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("state", sa.String(32), nullable=False, server_default="OFFLINE"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("current_call_id", sa.String(36), sa.ForeignKey("calls.id", ondelete="SET NULL"), nullable=True),
        sa.Column("last_heartbeat", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_paused", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("state_changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "state IN ('OFFLINE','AVAILABLE','RESERVED','DIALING','CONNECTED','WRAP_UP','PAUSED')",
            name="ck_agent_state",
        ),
    )
    op.create_index("ix_agents_campaign_state", "agents", ["campaign_id", "state"])
    op.create_index("ix_agents_heartbeat", "agents", ["last_heartbeat"])

    # ── borrowers ──
    op.create_table(
        "borrowers",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("campaign_id", sa.String(36), sa.ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False),
        sa.Column("synthetic_name", sa.String(255), nullable=False),
        sa.Column("masked_phone", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="QUEUED"),
        sa.Column("is_dnc", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("next_eligible_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('QUEUED','ALLOCATED','COMPLETED','FAILED','DNC','EXHAUSTED')",
            name="ck_borrower_status",
        ),
    )
    op.create_index("ix_borrowers_campaign_status_priority", "borrowers", ["campaign_id", "status", "priority"])
    op.create_index("ix_borrowers_next_eligible", "borrowers", ["next_eligible_at"])

    # ── dispatch_records ──
    op.create_table(
        "dispatch_records",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("call_id", sa.String(36), sa.ForeignKey("calls.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("campaign_id", sa.String(36), nullable=False),
        sa.Column("provider_id", sa.String(64), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("masked_phone", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="PENDING"),
        sa.Column("worker_id", sa.String(255), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("provider_call_id", sa.String(255), nullable=True),
        sa.Column("result_code", sa.String(64), nullable=True),
        sa.Column("result_detail", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('PENDING','CLAIMED','SENT','SUCCEEDED','FAILED','AMBIGUOUS')",
            name="ck_dispatch_status",
        ),
    )
    op.create_index("ix_dispatch_status_lease", "dispatch_records", ["status", "lease_expires_at"])

    # ── provider_event_inbox ──
    op.create_table(
        "provider_event_inbox",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("provider_id", sa.String(64), nullable=False),
        sa.Column("provider_event_id", sa.String(255), nullable=False),
        sa.Column("provider_call_id", sa.String(255), nullable=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("sequence_number", sa.BigInteger(), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_status", sa.String(32), nullable=False, server_default="PENDING"),
        sa.Column("processing_error", sa.Text(), nullable=True),
        sa.UniqueConstraint("provider_id", "provider_event_id", name="uq_provider_event"),
    )
    op.create_index("ix_event_inbox_processing_status", "provider_event_inbox", ["processing_status"])
    op.create_index("ix_event_inbox_call_id", "provider_event_inbox", ["provider_call_id"])

    # ── safety_decisions ──
    op.create_table(
        "safety_decisions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("campaign_id", sa.String(36), nullable=False),
        sa.Column("requested_count", sa.Integer(), nullable=False),
        sa.Column("approved_count", sa.Integer(), nullable=False),
        sa.Column("decision", sa.String(32), nullable=False),
        sa.Column("reason_codes", sa.JSON(), nullable=False),
        sa.Column("capacity_snapshot", sa.JSON(), nullable=False),
        sa.Column("provider_health_snapshot", sa.JSON(), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_safety_decisions_campaign", "safety_decisions", ["campaign_id", "decided_at"])

    # ── audit_events ──
    op.create_table(
        "audit_events",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("entity_type", sa.String(64), nullable=True),
        sa.Column("entity_id", sa.String(36), nullable=True),
        sa.Column("campaign_id", sa.String(36), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_audit_entity", "audit_events", ["entity_type", "entity_id"])
    op.create_index("ix_audit_campaign", "audit_events", ["campaign_id", "created_at"])
    op.create_index("ix_audit_event_type", "audit_events", ["event_type"])

    # ── provider_health ──
    op.create_table(
        "provider_health",
        sa.Column("provider_id", sa.String(64), primary_key=True),
        sa.Column("circuit_state", sa.String(32), nullable=False, server_default="CLOSED"),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("half_open_successes", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "circuit_state IN ('CLOSED','OPEN','HALF_OPEN')",
            name="ck_circuit_state",
        ),
    )


def downgrade() -> None:
    op.drop_table("provider_health")
    op.drop_table("audit_events")
    op.drop_table("safety_decisions")
    op.drop_table("provider_event_inbox")
    op.drop_table("dispatch_records")
    op.drop_table("borrowers")
    op.drop_table("agents")
    op.drop_table("calls")
    op.drop_table("campaigns")
