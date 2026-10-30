"""Align schema with the ORM models: provider_health columns and simulation_results.

001_initial created provider_health with column names that the ORM model (and
ProviderHealthService) never used, and did not create simulation_results. On
PostgreSQL every circuit-breaker read/write therefore failed.

Revision ID: 002_align_models
Revises: 001_initial
Create Date: 2026-10-29 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "002_align_models"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── provider_health: match app.persistence.models.ProviderHealthRecord ──
    op.alter_column("provider_health", "opened_at", new_column_name="circuit_opened_at")
    op.alter_column("provider_health", "half_open_successes", new_column_name="half_open_probes_sent")
    op.add_column(
        "provider_health",
        sa.Column("total_requests", sa.BigInteger(), nullable=False, server_default="0"),
    )
    op.add_column(
        "provider_health",
        sa.Column("total_failures", sa.BigInteger(), nullable=False, server_default="0"),
    )

    # ── simulation_results ──
    op.create_table(
        "simulation_results",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("scenario_name", sa.String(255), nullable=False),
        sa.Column("dialing_mode", sa.String(32), nullable=False),
        sa.Column("random_seed", sa.Integer(), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("metrics", sa.JSON(), nullable=False),
        sa.Column("invariant_violations", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("ran_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("simulation_results")
    op.drop_column("provider_health", "total_failures")
    op.drop_column("provider_health", "total_requests")
    op.alter_column("provider_health", "half_open_probes_sent", new_column_name="half_open_successes")
    op.alter_column("provider_health", "circuit_opened_at", new_column_name="opened_at")
