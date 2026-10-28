"""SQLAlchemy engine and session factories."""
from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator

from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_settings = get_settings()

from sqlalchemy.pool import StaticPool

def _make_sync_engine(url: str, **kwargs):
    if "sqlite" in url:
        kw = {"echo": False}
        if ":memory:" in url:
            kw["poolclass"] = StaticPool
            kw["connect_args"] = {"check_same_thread": False}
        return create_engine(url, **kw)
    return create_engine(
        url,
        pool_size=kwargs.get("pool_size", _settings.db_pool_size),
        max_overflow=kwargs.get("max_overflow", _settings.db_max_overflow),
        pool_timeout=kwargs.get("pool_timeout", _settings.db_pool_timeout),
        pool_pre_ping=True,
        echo=False,
    )


def _make_async_engine(url: str, **kwargs):
    if "sqlite" in url:
        kw = {"echo": False}
        if ":memory:" in url:
            kw["poolclass"] = StaticPool
        return create_async_engine(url, **kw)
    return create_async_engine(
        url,
        pool_size=kwargs.get("pool_size", _settings.db_pool_size),
        max_overflow=kwargs.get("max_overflow", _settings.db_max_overflow),
        pool_timeout=kwargs.get("pool_timeout", _settings.db_pool_timeout),
        pool_pre_ping=True,
        echo=False,
    )


class _SessionProxy:
    def __init__(self, maker):
        self._maker = maker

    def __call__(self, *args, **kwargs):
        return self._maker(*args, **kwargs)

    def update(self, maker):
        self._maker = maker

    def __getattr__(self, name):
        return getattr(self._maker, name)


# ── Synchronous engine (for migrations, tests, reconciliation) ───────────────
sync_engine = _make_sync_engine(_settings.database_url)

_sync_maker = sessionmaker(
    bind=sync_engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)
SyncSessionLocal = _SessionProxy(_sync_maker)

# ── Async engine (for API, workers, event processor) ────────────────────────
async_engine = _make_async_engine(_settings.database_url_async)

_async_maker = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)
AsyncSessionLocal = _SessionProxy(_async_maker)


@contextmanager
def get_sync_db() -> Generator[Session, None, None]:
    """Provide a synchronous database session."""
    session = SyncSessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@asynccontextmanager
async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Provide an async database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def check_database_health() -> bool:
    """Return True if the database is reachable."""
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.warning("database_health_check_failed", error=str(exc))
        return False


def override_engines(sync_url: str, async_url: str) -> None:
    """Replace engines — used in tests to point at the test database."""
    global sync_engine, async_engine  # noqa: PLW0603

    sync_engine = _make_sync_engine(sync_url, pool_size=5, max_overflow=10)
    new_sync_maker = sessionmaker(
        bind=sync_engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )
    SyncSessionLocal.update(new_sync_maker)

    async_engine = _make_async_engine(async_url, pool_size=5, max_overflow=10)
    new_async_maker = async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    AsyncSessionLocal.update(new_async_maker)
    return sync_engine, async_engine


def get_sync_engine():
    return sync_engine


def get_async_engine():
    return async_engine

