"""Integration tests for FastAPI endpoints."""
from __future__ import annotations

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.main import app
from app.persistence.models import Campaign


@pytest.mark.asyncio
class TestAPIEndpoints:
    async def test_health_check(self) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] in ("ok", "healthy", "degraded", "unhealthy")

    async def test_metrics_endpoint(self) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/metrics")
            assert resp.status_code == 200
            assert "text/plain" in resp.headers.get("content-type", "")

    async def test_campaign_crud(self, db_session: AsyncSession) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # 1. Create campaign
            payload = {
                "name": "API Test Campaign",
                "dialing_mode": "progressive",
                "max_concurrent_calls": 20,
            }
            create_resp = await client.post("/api/v1/campaigns", json=payload)
            assert create_resp.status_code == 201
            camp_data = create_resp.json()
            camp_id = camp_data["id"]
            assert camp_data["name"] == "API Test Campaign"

            # 2. Get campaign
            get_resp = await client.get(f"/api/v1/campaigns/{camp_id}")
            assert get_resp.status_code == 200
            assert get_resp.json()["id"] == camp_id

    async def test_agent_registration_and_heartbeat(
        self, db_session: AsyncSession, sample_campaign: Campaign
    ) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # Register agent
            agent_payload = {
                "campaign_id": sample_campaign.id,
                "name": "API-Agent-01",
            }
            create_resp = await client.post("/api/v1/agents", json=agent_payload)
            assert create_resp.status_code == 201
            agent_data = create_resp.json()
            agent_id = agent_data["id"]
            assert agent_data["name"] == "API-Agent-01"

            # Send heartbeat
            hb_resp = await client.post(f"/api/v1/agents/{agent_id}/heartbeat")
            assert hb_resp.status_code == 200
            assert hb_resp.json()["ok"] is True

    async def test_reconciliation_trigger(self, db_session: AsyncSession) -> None:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/api/v1/reconcile")
            assert resp.status_code == 200
            data = resp.json()
            assert "wrap_up_released" in data
