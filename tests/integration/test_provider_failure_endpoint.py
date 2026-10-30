"""POST /simulation/provider-failure injects failures into the circuit breaker,
and /dial then refuses to dial through that provider."""
from __future__ import annotations

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from app.providers.mock_provider_a import MockProviderA
from app.providers.mock_provider_b import MockProviderB
from app.workers.runtime import DialerRuntime


@pytest_asyncio.fixture
async def client(db_session: AsyncSession):
    from app.main import app

    app.state.runtime = DialerRuntime({
        "mock_provider_a": MockProviderA(seed=1, delay_scale=0.01),
        "mock_provider_b": MockProviderB(seed=1, delay_scale=0.01),
    })
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test/api/v1") as c:
            yield c
    finally:
        await app.state.runtime.stop()
        app.state.runtime = None


async def _active_campaign(client) -> str:
    cid = (await client.post("/campaigns", json={"name": "pf", "max_concurrent_calls": 5})).json()["id"]
    for i in range(2):
        agent = (await client.post("/agents", json={"campaign_id": cid, "name": f"A{i}"})).json()
        await client.post(f"/agents/{agent['id']}/availability", json={"state": "AVAILABLE"})
        await client.post("/borrowers", json={"campaign_id": cid, "synthetic_name": f"B{i}", "masked_phone": f"***-{i}"})
    await client.post(f"/campaigns/{cid}/start")
    return cid


@pytest.mark.asyncio
async def test_injected_failures_open_circuit_and_block_dialing(client) -> None:
    cid = await _active_campaign(client)

    resp = await client.post("/simulation/provider-failure",
                             params={"provider_id": "mock_provider_b", "failure_count": 2})
    assert resp.status_code == 200, resp.text
    assert resp.json()["circuit_state"] == "CLOSED"  # below the threshold (5)

    resp = await client.post("/simulation/provider-failure",
                             params={"provider_id": "mock_provider_b", "failure_count": 3})
    body = resp.json()
    assert body["circuit_state"] == "OPEN"
    assert body["provider_health"]["consecutive_failures"] == 5

    dial = (await client.post(f"/campaigns/{cid}/dial", params={"provider_id": "mock_provider_b"})).json()
    assert dial["calls_allocated"] == 0
    assert dial["safety_decision"]["decision"] == "REJECT"
    assert "CIRCUIT_BREAKER_OPEN" in dial["safety_decision"]["reason_codes"]
    assert dial["safety_decision"]["audited"] is True

    # The other provider is unaffected.
    dial = (await client.post(f"/campaigns/{cid}/dial", params={"provider_id": "mock_provider_a"})).json()
    assert dial["calls_allocated"] == 2


@pytest.mark.asyncio
async def test_rejects_unknown_provider_and_bad_counts(client) -> None:
    assert (await client.post("/simulation/provider-failure", params={"provider_id": "nope"})).status_code == 400
    assert (await client.post("/simulation/provider-failure",
                              params={"provider_id": "mock_provider_a", "failure_count": 0})).status_code == 422
