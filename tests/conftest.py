"""Global pytest fixtures and test database setup."""
from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from typing import AsyncGenerator, Generator
import uuid

# Tests must not depend on a developer's local .env (environment variables take
# precedence over it): unauthenticated provider webhooks, test environment.
os.environ["APP_ENV"] = "test"
os.environ["PROVIDER_WEBHOOK_SECRETS"] = "{}"

# Every per-test SQLite file lives here; the directory is removed when the
# session ends. (A test that touches the database without the db_session
# fixture — e.g. GET /health — reconnects to the previous test's deleted file,
# and SQLite recreates it empty; those files used to pile up in the repo root.)
_TEST_DB_DIR = tempfile.mkdtemp(prefix="smartdialer-tests-")


def pytest_sessionfinish(session, exitstatus) -> None:
    shutil.rmtree(_TEST_DB_DIR, ignore_errors=True)

import pytest
import pytest_asyncio
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.core.config import get_settings
from app.core.enums import AgentState, BorrowerStatus
from app.persistence.database import override_engines
from app.persistence.models import Agent, Base, Borrower, Campaign
from app.providers.base import (
    ProviderAdapter,
    ProviderCallRequest,
    ProviderCallResponse,
    ProviderEvent,
    ProviderHealthStatus,
)


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for each test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a fresh SQLite database session for each test."""
    db_id = uuid.uuid4().hex[:8]
    test_db_file = os.path.join(_TEST_DB_DIR, f"test_{db_id}.db")
    test_sync_url = f"sqlite:///{test_db_file}"
    test_async_url = f"sqlite+aiosqlite:///{test_db_file}"

    sync_eng = create_engine(
        test_sync_url,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=sync_eng)

    override_engines(test_sync_url, test_async_url)

    from app.persistence.database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        yield session

    sync_eng.dispose()
    from app.persistence.database import async_engine
    await async_engine.dispose()

    if os.path.exists(test_db_file):
        try:
            os.remove(test_db_file)
        except OSError:
            pass


@pytest_asyncio.fixture
async def sample_campaign(db_session: AsyncSession) -> Campaign:
    """Fixture providing a sample active campaign."""
    campaign = Campaign(
        id=str(uuid.uuid4()),
        name="Test Campaign",
        status="ACTIVE",
        dialing_mode="progressive",
        max_concurrent_calls=10,
        max_attempts_per_borrower=3,
        retry_cooldown_seconds=60,
        wrap_up_seconds=15,
    )
    db_session.add(campaign)
    await db_session.commit()
    await db_session.refresh(campaign)
    return campaign


@pytest_asyncio.fixture
async def sample_agent(db_session: AsyncSession, sample_campaign: Campaign) -> Agent:
    """Fixture providing an available agent."""
    agent = Agent(
        id=str(uuid.uuid4()),
        campaign_id=sample_campaign.id,
        name="Agent-001",
        state=AgentState.AVAILABLE,
    )
    db_session.add(agent)
    await db_session.commit()
    await db_session.refresh(agent)
    return agent


class StubProvider(ProviderAdapter):
    """Deterministic in-memory provider for failure-injection tests.

    Emits no events on its own (tests push events in whatever order they need).
    mode: "accept" | "reject" | "timeout" (never accepted) |
          "timeout_after_accept" (accepted, but the response is lost).
    """

    def __init__(self, provider_id: str = "mock_provider_a", *, mode: str = "accept") -> None:
        self._provider_id = provider_id
        self.mode = mode
        self.requests: list[str] = []
        self.accepted: dict[str, str] = {}
        self.event_queue: asyncio.Queue = asyncio.Queue()

    @property
    def provider_id(self) -> str:
        return self._provider_id

    async def initiate_call(self, request: ProviderCallRequest) -> ProviderCallResponse:
        key = request.idempotency_key
        self.requests.append(key)
        if key in self.accepted:
            return ProviderCallResponse(accepted=True, provider_call_id=self.accepted[key])
        if self.mode == "reject":
            return ProviderCallResponse(accepted=False, provider_call_id=None, error_code="PROVIDER_REJECTED")
        if self.mode == "timeout":
            raise asyncio.TimeoutError("stub timeout")
        provider_call_id = f"stub-{len(self.accepted) + 1}-{key[-6:]}"
        self.accepted[key] = provider_call_id
        if self.mode == "timeout_after_accept":
            raise asyncio.TimeoutError("stub timeout after accept")
        return ProviderCallResponse(accepted=True, provider_call_id=provider_call_id)

    async def get_call_status(self, provider_call_id: str):
        return None

    async def find_call_by_idempotency_key(self, idempotency_key: str) -> str | None:
        return self.accepted.get(idempotency_key)

    async def cancel_call(self, provider_call_id: str) -> bool:
        return False

    async def health_check(self) -> ProviderHealthStatus:
        return ProviderHealthStatus(self._provider_id, True, 1.0, 0.0, "CLOSED")

    def normalize_event(self, raw: dict) -> ProviderEvent:
        raise NotImplementedError


async def allocate_and_dispatch(campaign_id: str, provider: ProviderAdapter, count: int = 1) -> list[str]:
    """Reserve `count` calls through the real allocator and dispatch them through
    the real DispatchWorker. Returns the call ids."""
    from app.persistence.database import AsyncSessionLocal
    from app.services.call_allocator import CallAllocator
    from app.workers.dispatch_worker import DispatchWorker

    async with AsyncSessionLocal() as session:
        calls = await CallAllocator(provider_id=provider.provider_id).allocate_batch(session, campaign_id, count)
        await session.commit()
    await DispatchWorker(provider).run_once(AsyncSessionLocal, batch_size=count)
    return [c.id for c in calls]


async def wait_until(predicate, *, timeout: float = 10.0, interval: float = 0.05):
    """Await an async predicate until it returns a truthy value (bounded)."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        value = await predicate()
        if value:
            return value
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(interval)


@pytest.fixture
def stub_provider() -> StubProvider:
    return StubProvider()


@pytest_asyncio.fixture
async def sample_borrower(db_session: AsyncSession, sample_campaign: Campaign) -> Borrower:
    """Fixture providing a queued borrower."""
    borrower = Borrower(
        id=str(uuid.uuid4()),
        campaign_id=sample_campaign.id,
        synthetic_name="Test Borrower",
        masked_phone="+15551234567",
        status=BorrowerStatus.QUEUED,
        attempt_count=0,
        max_attempts=3,
    )
    db_session.add(borrower)
    await db_session.commit()
    await db_session.refresh(borrower)
    return borrower
