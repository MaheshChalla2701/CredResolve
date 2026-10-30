"""UTC time helpers.

All timestamps are stored as UTC. PostgreSQL returns timezone-aware values, but
SQLite returns naive ones even for DateTime(timezone=True) columns, so any
Python-side comparison must normalise first.
"""
from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: datetime) -> datetime:
    """Return value as an aware UTC datetime (naive values are assumed to be UTC)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
