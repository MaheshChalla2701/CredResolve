"""Safety boundary: what a direct CallAllocator call can and cannot do, and which
modules may reach the allocator at all."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import CampaignStatus
from app.core.errors import CampaignConcurrencyLimitError, CampaignNotActiveError
from app.persistence.models import Agent, Borrower, Call, Campaign
from app.services.call_allocator import CallAllocator

APP_DIR = Path(__file__).resolve().parents[2] / "app"
# The dialing service is the boundary: it runs the Safety Controller before
# allocating. Nothing else in the application may import the allocator.
ALLOWED_ALLOCATOR_IMPORTERS = {
    "app/services/call_allocator.py",
    "app/services/dialing.py",
    "app/services/progressive_dialer.py",
    "app/services/predictive_dialer.py",
}


def _imports_allocator(path: Path) -> bool:
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "app.services.call_allocator":
            return True
        if isinstance(node, ast.Import) and any(a.name == "app.services.call_allocator" for a in node.names):
            return True
    return False


def test_only_dialing_services_import_the_allocator() -> None:
    importers = {
        str(p.relative_to(APP_DIR.parent))
        for p in APP_DIR.rglob("*.py")
        if _imports_allocator(p)
    }
    assert importers <= ALLOWED_ALLOCATOR_IMPORTERS, importers - ALLOWED_ALLOCATOR_IMPORTERS


@pytest.mark.asyncio
class TestAllocatorHardLimits:
    async def test_refuses_campaign_that_is_not_active(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_agent: Agent, sample_borrower: Borrower
    ) -> None:
        sample_campaign.status = CampaignStatus.PAUSED
        await db_session.commit()
        with pytest.raises(CampaignNotActiveError):
            await CallAllocator().allocate_one(db_session, sample_campaign.id)
        assert (await db_session.execute(select(Call))).first() is None

    async def test_refuses_beyond_concurrency_cap(
        self, db_session: AsyncSession, sample_campaign: Campaign, sample_borrower: Borrower
    ) -> None:
        sample_campaign.max_concurrent_calls = 1
        for i in range(2):
            db_session.add(Agent(campaign_id=sample_campaign.id, name=f"A{i}", state="AVAILABLE"))
        db_session.add(Borrower(campaign_id=sample_campaign.id, synthetic_name="B2", masked_phone="***-2",
                                status="QUEUED"))
        await db_session.commit()

        allocator = CallAllocator()
        await allocator.allocate_one(db_session, sample_campaign.id)
        with pytest.raises(CampaignConcurrencyLimitError):
            await allocator.allocate_one(db_session, sample_campaign.id)
        outcome = await allocator.allocate_up_to(db_session, sample_campaign.id, 5)
        assert outcome.calls == [] and outcome.stop_reason == "CAMPAIGN_CONCURRENCY_LIMIT"
