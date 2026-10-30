"""Transient-conflict classification and the retrying transaction helper."""
from __future__ import annotations

import sqlite3

import pytest
from sqlalchemy.exc import OperationalError

from app.persistence.retry import is_transient_db_error, run_transaction


class _PgError(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(f"pg error {sqlstate}")
        self.sqlstate = sqlstate


def _wrapped(orig: BaseException) -> OperationalError:
    """How SQLAlchemy surfaces a DBAPI error."""
    return OperationalError("UPDATE ...", {}, orig)


class TestIsTransient:
    @pytest.mark.parametrize("sqlstate", ["40P01", "40001"])
    def test_deadlock_and_serialization_failure(self, sqlstate: str) -> None:
        assert is_transient_db_error(_wrapped(_PgError(sqlstate)))

    def test_found_through_exception_chain(self) -> None:
        try:
            try:
                raise _PgError("40P01")
            except _PgError as inner:
                raise RuntimeError("outer") from inner
        except RuntimeError as exc:
            assert is_transient_db_error(exc)

    def test_sqlite_busy(self) -> None:
        assert is_transient_db_error(_wrapped(sqlite3.OperationalError("database is locked")))

    @pytest.mark.parametrize("exc", [
        _wrapped(_PgError("23505")),                       # unique violation
        _wrapped(sqlite3.OperationalError("no such table: calls")),
        ValueError("bug"),
    ])
    def test_other_errors_are_not_transient(self, exc: BaseException) -> None:
        assert not is_transient_db_error(exc)


class _FakeSession:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


@pytest.mark.asyncio
class TestRunTransaction:
    async def test_retries_transient_conflict_then_commits(self) -> None:
        session, calls = _FakeSession(), []

        async def work(s):
            calls.append(1)
            if len(calls) < 3:
                raise _wrapped(_PgError("40P01"))
            return "done"

        assert await run_transaction(session, work, op="t", base_delay=0) == "done"
        assert (len(calls), session.rollbacks, session.commits) == (3, 2, 1)

    async def test_gives_up_after_attempts(self) -> None:
        session = _FakeSession()

        async def work(s):
            raise _wrapped(_PgError("40001"))

        with pytest.raises(OperationalError):
            await run_transaction(session, work, op="t", attempts=2, base_delay=0)
        assert (session.rollbacks, session.commits) == (2, 0)

    async def test_non_transient_error_is_not_retried(self) -> None:
        session, calls = _FakeSession(), []

        async def work(s):
            calls.append(1)
            raise ValueError("bug")

        with pytest.raises(ValueError):
            await run_transaction(session, work, op="t", base_delay=0)
        assert (len(calls), session.rollbacks, session.commits) == (1, 1, 0)
