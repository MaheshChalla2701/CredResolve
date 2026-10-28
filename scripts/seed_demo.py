"""Demo seed script — populates database with realistic sample campaigns, agents, and borrowers.

Run this to explore the system via Swagger UI (http://localhost:8000/docs).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import uuid
from faker import Faker

from app.core.enums import AgentState, BorrowerStatus
from app.core.logging import configure_logging, get_logger
from app.persistence.database import AsyncSessionLocal, check_database_health, override_engines
from app.persistence.models import Agent, Base, Borrower, Campaign

logger = get_logger(__name__)


async def seed() -> None:
    fake = Faker()
    Faker.seed(1337)

    async with AsyncSessionLocal() as session:
        # Create Progressive Campaign
        prog_campaign = Campaign(
            id=str(uuid.uuid4()),
            name="Collections Campaign Alpha (Progressive)",
            status="ACTIVE",
            dialing_mode="progressive",
            max_concurrent_calls=25,
            max_attempts_per_borrower=3,
            retry_cooldown_seconds=3600,
            wrap_up_seconds=30,
        )
        session.add(prog_campaign)

        # Create Predictive Campaign
        pred_campaign = Campaign(
            id=str(uuid.uuid4()),
            name="Collections Campaign Beta (Predictive)",
            status="ACTIVE",
            dialing_mode="predictive",
            max_concurrent_calls=40,
            max_attempts_per_borrower=3,
            retry_cooldown_seconds=3600,
            wrap_up_seconds=30,
        )
        session.add(pred_campaign)

        print(f"Created Campaigns:")
        print(f"  - Progressive: {prog_campaign.name} (ID: {prog_campaign.id})")
        print(f"  - Predictive:  {pred_campaign.name} (ID: {pred_campaign.id})")

        # Seed agents for progressive campaign
        for i in range(10):
            agent = Agent(
                id=str(uuid.uuid4()),
                campaign_id=prog_campaign.id,
                name=f"Agent-Alpha-{i+1:02d}",
                state=AgentState.AVAILABLE if i < 8 else AgentState.PAUSED,
                is_paused=i >= 8,
            )
            session.add(agent)

        # Seed agents for predictive campaign
        for i in range(15):
            agent = Agent(
                id=str(uuid.uuid4()),
                campaign_id=pred_campaign.id,
                name=f"Agent-Beta-{i+1:02d}",
                state=AgentState.AVAILABLE if i < 12 else AgentState.PAUSED,
                is_paused=i >= 12,
            )
            session.add(agent)

        # Seed synthetic borrowers for both campaigns
        for campaign in [prog_campaign, pred_campaign]:
            for j in range(50):
                borrower = Borrower(
                    id=str(uuid.uuid4()),
                    campaign_id=campaign.id,
                    synthetic_name=fake.name(),
                    masked_phone=f"+1-555-{fake.numerify('###-####')}",
                    status=BorrowerStatus.QUEUED,
                    priority=fake.random_int(min=0, max=10),
                    attempt_count=0,
                    max_attempts=3,
                )
                session.add(borrower)

        await session.commit()

    print("\nDatabase successfully seeded!")
    print("\nNext steps:")
    print("  1. Start server: py -3.13 -m uvicorn app.main:app --reload")
    print("  2. Open Swagger UI: http://localhost:8000/docs")
    print("  3. Trigger dial cycles: POST /api/v1/campaigns/{id}/dial")


async def main_async() -> None:
    parser = argparse.ArgumentParser(description="Seed demo data")
    parser.add_argument("--db-path", default=None, help="SQLite DB path (default: PostgreSQL or smartdialer.db fallback)")
    args = parser.parse_args()

    configure_logging("INFO")

    if args.db_path:
        db_file = os.path.abspath(args.db_path)
        sync_url = f"sqlite:///{db_file}"
        async_url = f"sqlite+aiosqlite:///{db_file}"
        new_sync, _ = override_engines(sync_url, async_url)
        Base.metadata.create_all(bind=new_sync)
        print(f"Using SQLite database: {db_file}")
    else:
        # Check if default postgres is reachable
        is_healthy = await check_database_health()
        if not is_healthy:
            db_file = os.path.abspath("smartdialer.db")
            sync_url = f"sqlite:///{db_file}"
            async_url = f"sqlite+aiosqlite:///{db_file}"
            new_sync, _ = override_engines(sync_url, async_url)
            Base.metadata.create_all(bind=new_sync)
            print(f"PostgreSQL not reachable; fell back to SQLite: {db_file}")

    await seed()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
