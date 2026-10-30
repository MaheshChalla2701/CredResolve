"""End-to-end API regression tests: POST /dial → dispatch worker → mock provider →
provider events → final call/agent/borrower states, with the background runtime
running exactly as it does under uvicorn (mock providers only)."""
from __future__ import annotations

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from app.providers.mock_provider_a import MockProviderA
from app.providers.mock_provider_b import MockProviderB
from app.workers.runtime import DialerRuntime
from conftest import wait_until


class SpyingMockProviderA(MockProviderA):
    """The real mock provider, recording every initiate_call request."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.initiated_keys: list[str] = []

    async def initiate_call(self, request):
        self.initiated_keys.append(request.idempotency_key)
        return await super().initiate_call(request)


def _providers(answer_rate: float) -> dict:
    return {
        "mock_provider_a": SpyingMockProviderA(
            seed=11, success_rate=1.0, timeout_rate=0.0, answer_rate=answer_rate, delay_scale=0.01
        ),
        "mock_provider_b": MockProviderB(
            seed=11, success_rate=1.0, timeout_rate=0.0, answer_rate=answer_rate, delay_scale=0.01
        ),
    }


@pytest_asyncio.fixture
async def live_api(db_session: AsyncSession, request):
    """Run the app (lifespan included) with deterministic mock providers."""
    from app.main import app

    answer_rate = getattr(request, "param", 1.0)
    runtime = DialerRuntime(_providers(answer_rate), dispatch_interval=0.05, maintenance_interval=0.1)
    app.state.runtime = runtime
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test/api/v1") as client:
            yield client, runtime


async def _setup_campaign(client, *, mode: str, agents: int = 2, borrowers: int = 3, **campaign) -> tuple[str, list[str]]:
    body = {"name": f"API-{mode}", "dialing_mode": mode, "max_concurrent_calls": 10,
            "wrap_up_seconds": 0, "retry_cooldown_seconds": 0, **campaign}
    resp = await client.post("/campaigns", json=body)
    assert resp.status_code == 201, resp.text
    cid = resp.json()["id"]
    agent_ids = []
    for i in range(agents):
        a = (await client.post("/agents", json={"campaign_id": cid, "name": f"Agent-{i}"})).json()
        r = await client.post(f"/agents/{a['id']}/availability", json={"state": "AVAILABLE"})
        assert r.status_code == 200, r.text
        agent_ids.append(a["id"])
    for i in range(borrowers):
        r = await client.post("/borrowers", json={"campaign_id": cid, "synthetic_name": f"B{i}", "masked_phone": f"***-{i:04d}"})
        assert r.status_code == 201, r.text
    assert (await client.post(f"/campaigns/{cid}/start")).json()["status"] == "ACTIVE"
    return cid, agent_ids


async def _wait_terminal(client, call_ids: list[str]) -> list[dict]:
    async def all_terminal():
        calls = [(await client.get(f"/calls/{cid}")).json() for cid in call_ids]
        return calls if all(c["state"] in ("COMPLETED", "FAILED", "CANCELLED") for c in calls) else None
    return await wait_until(all_terminal, timeout=15)


async def _wait_agents_available(client, agent_ids: list[str]) -> None:
    async def all_available():
        states = [(await client.get(f"/agents/{a}")).json()["state"] for a in agent_ids]
        return all(s == "AVAILABLE" for s in states)
    await wait_until(all_available, timeout=10)


@pytest.mark.asyncio
async def test_progressive_dial_reaches_mock_provider_and_completes(live_api) -> None:
    client, runtime = live_api
    provider = runtime.providers["mock_provider_a"]
    cid, agent_ids = await _setup_campaign(client, mode="progressive")

    resp = await client.post(f"/campaigns/{cid}/dial")
    assert resp.status_code == 200, resp.text
    dial = resp.json()
    assert dial["dialing_mode"] == "progressive"
    assert dial["calls_allocated"] == 2
    assert dial["call_state"] == "RESERVED" and dial["dispatch_status"] == "PENDING"
    assert dial["dispatch_worker_running"] is True
    assert dial["safety_decision"]["decision"] == "APPROVE"

    calls = await _wait_terminal(client, dial["call_ids"])
    for call in calls:
        assert call["state"] == "COMPLETED"
        assert call["dispatch_status"] == "SUCCEEDED"
        assert call["dispatch_attempts"] == 1
        assert call["provider_call_id"].startswith("pA-")
        assert call["answered_at"] and call["connected_at"] and call["completed_at"]
    # The real mock provider was invoked exactly once per call (no duplicates).
    assert sorted(provider.initiated_keys) == sorted(c["idempotency_key"] for c in calls)

    # Wrap-up (0s) is released by the background reconciliation loop.
    await _wait_agents_available(client, agent_ids)
    borrowers = (await client.get(f"/campaigns/{cid}/borrowers")).json()
    assert sorted(b["status"] for b in borrowers) == ["COMPLETED", "COMPLETED", "QUEUED"]


@pytest.mark.asyncio
@pytest.mark.parametrize("live_api", [0.0], indirect=True)
async def test_unanswered_calls_free_agents_and_requeue_borrowers(live_api) -> None:
    client, runtime = live_api
    cid, agent_ids = await _setup_campaign(client, mode="progressive", agents=2, borrowers=2,
                                           max_attempts_per_borrower=2)

    dial = (await client.post(f"/campaigns/{cid}/dial")).json()
    calls = await _wait_terminal(client, dial["call_ids"])
    assert {c["state"] for c in calls} == {"FAILED"}
    assert {c["failure_reason"] for c in calls} <= {"no_answer", "busy", "failed"}
    await _wait_agents_available(client, agent_ids)
    borrowers = (await client.get(f"/campaigns/{cid}/borrowers")).json()
    assert [(b["status"], b["attempt_count"]) for b in borrowers] == [("QUEUED", 1), ("QUEUED", 1)]

    # The same agents are reused for the retry; the retry limit (2) then exhausts them.
    dial2 = (await client.post(f"/campaigns/{cid}/dial")).json()
    assert dial2["calls_allocated"] == 2
    await _wait_terminal(client, dial2["call_ids"])
    await _wait_agents_available(client, agent_ids)
    borrowers = (await client.get(f"/campaigns/{cid}/borrowers")).json()
    assert [(b["status"], b["attempt_count"]) for b in borrowers] == [("EXHAUSTED", 2), ("EXHAUSTED", 2)]

    dial3 = (await client.post(f"/campaigns/{cid}/dial")).json()
    assert dial3["calls_allocated"] == 0


@pytest.mark.asyncio
async def test_predictive_campaign_uses_pacing_engine_and_safety(live_api) -> None:
    client, runtime = live_api
    cid, _ = await _setup_campaign(client, mode="predictive", agents=4, borrowers=6)

    dial = (await client.post(f"/campaigns/{cid}/dial", params={"provider_id": "mock_provider_b"})).json()
    assert dial["dialing_mode"] == "predictive"
    pacing = dial["pacing"]
    assert pacing is not None and pacing["available_agents"] == 4
    # The pacer only proposes; the Safety Controller saw exactly that proposal.
    assert dial["safety_decision"]["requested_count"] == pacing["recommended_count"]
    assert dial["calls_allocated"] <= dial["safety_decision"]["approved_count"] <= 4
    assert dial["calls_allocated"] >= 1

    calls = await _wait_terminal(client, dial["call_ids"])
    assert all(c["provider_call_id"].startswith("pB-") for c in calls)
    assert all(c["dispatch_status"] == "SUCCEEDED" for c in calls)


@pytest.mark.asyncio
async def test_dial_rejects_unknown_provider(live_api) -> None:
    client, _ = live_api
    cid, _ = await _setup_campaign(client, mode="progressive")
    resp = await client.post(f"/campaigns/{cid}/dial", params={"provider_id": "real_carrier"})
    assert resp.status_code == 400
    assert "Unknown provider_id" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_duplicate_webhook_does_not_change_state_twice(live_api) -> None:
    client, runtime = live_api
    cid, _ = await _setup_campaign(client, mode="progressive", agents=1, borrowers=1)
    dial = (await client.post(f"/campaigns/{cid}/dial")).json()
    call = (await _wait_terminal(client, dial["call_ids"]))[0]

    params = {"provider_id": "mock_provider_a", "provider_event_id": "dup-evt-1",
              "provider_call_id": call["provider_call_id"], "event_type": "answered"}
    first = (await client.post("/provider-events", params=params)).json()
    second = (await client.post("/provider-events", params=params)).json()
    assert first["state_changed"] is False  # call already terminal: no regression
    assert second["state_changed"] is False
    assert (await client.get(f"/calls/{call['id']}")).json()["state"] == "COMPLETED"
