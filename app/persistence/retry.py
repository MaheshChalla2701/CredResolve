"""Run a unit of work in its own transaction, retrying transient conflicts.

PostgreSQL aborts one of two deadlocked transactions (SQLSTATE 40P01) and raises
serialization failures (40001) under REPEATABLE READ / SERIALIZABLE. Both mean
"nothing was written; run the same unit of work again". SQLite reports a writer
that waited past its busy timeout as "database is locked". Any other error is
rolled back and re-raised — it is never retried silently.

Lock order used by every write path (acquire in this order, never the reverse):

    campaign  →  call  →  dispatch record  →  agent  →  borrower  →  provider health

Allocation is the one path that starts at the campaign row; it then claims
agents and borrowers with SKIP LOCKED (it never waits on them) and only inserts
new call/dispatch rows. Paths that touch only agents (wrap-up release, leaked
agents, availability changes) lock just the agent row.
"""
from __future__ import annotations

import asyncio
import random
import sqlite3
from typing import TYPE_CHECKING, Any, Awaitable, Callable, TypeVar

from app.core.logging import get_logger

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)

T = TypeVar("T")

TRANSIENT_SQLSTATES = frozenset({"40P01", "40001"})  # deadlock_detected, serialization_failure


def is_transient_db_error(exc: BaseException) -> bool:
    """True for errors that mean "roll back and try the same work again"."""
    seen: set[int] = set()
    stack: list[Any] = [exc]
    while stack:
        err = stack.pop()
        if err is None or id(err) in seen:
            continue
        seen.add(id(err))
        if getattr(err, "sqlstate", None) in TRANSIENT_SQLSTATES or getattr(err, "pgcode", None) in TRANSIENT_SQLSTATES:
            return True
        if isinstance(err, sqlite3.OperationalError) and "database is locked" in str(err):
            return True
        stack.extend([getattr(err, "orig", None), err.__cause__, err.__context__])
    return False


async def run_transaction(
    session: "AsyncSession",
    work: Callable[["AsyncSession"], Awaitable[T]],
    *,
    op: str,
    attempts: int = 3,
    base_delay: float = 0.05,
) -> T:
    """Run work(session) and commit. On a transient conflict roll back and run it
    again (up to `attempts` times); on any other error roll back and re-raise.

    `work` must re-read what it needs: a rollback expires every object loaded
    in the session.
    """
    for attempt in range(1, attempts + 1):
        try:
            result = await work(session)
            await session.commit()
            return result
        except Exception as exc:
            await session.rollback()
            if attempt < attempts and is_transient_db_error(exc):
                logger.warning("db_transaction_retry", op=op, attempt=attempt, error=type(exc).__name__)
                await asyncio.sleep(base_delay * attempt * (1 + random.random()))
                continue
            raise
    raise AssertionError("unreachable")
