"""Global pytest fixtures and test database setup."""
from __future__ import annotations

import asyncio
from typing import AsyncGenerator, Generator
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.core.config import get_settings
from app.core.enums import AgentState, BorrowerStatus
from app.persistence.database import override_engines
from app.persistence.models import Agent, Base, Borrower, Campaign


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for each test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


import os

@pytest_asyncio.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a fresh SQLite database session for each test."""
    db_id = uuid.uuid4().hex[:8]
    test_db_file = os.path.abspath(f"test_{db_id}.db")
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
