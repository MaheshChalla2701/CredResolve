"""Provider webhook authentication (per-provider HMAC shared secret)."""
from __future__ import annotations

import json
import time
from urllib.parse import urlencode

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.webhook_auth import SIGNATURE_HEADER, TIMESTAMP_HEADER, sign
from app.core.config import get_settings

PATH = "/api/v1/provider-events"
SECRETS = {"mock_provider_a": "secret-a", "mock_provider_b": "secret-b"}


def _params(event_id: str = "evt-1", provider_id: str = "mock_provider_a") -> dict:
    return {"provider_id": provider_id, "provider_event_id": event_id,
            "provider_call_id": "unknown-call", "event_type": "ringing"}


def _signed(secret: str, params: dict, body: bytes = b"", *, ts: int | None = None) -> tuple[str, bytes, dict]:
    query = urlencode(params)
    timestamp = str(ts if ts is not None else int(time.time()))
    headers = {TIMESTAMP_HEADER: timestamp, SIGNATURE_HEADER: sign(secret, timestamp, "POST", PATH, query, body)}
    if body:
        headers["Content-Type"] = "application/json"
    return f"{PATH}?{query}", body, headers


@pytest_asyncio.fixture
async def client(db_session: AsyncSession):
    from app.main import app

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
def secrets_configured(monkeypatch):
    monkeypatch.setattr(get_settings(), "provider_webhook_secrets", dict(SECRETS))


@pytest.mark.asyncio
class TestWebhookAuth:
    async def test_development_without_secrets_accepts_unsigned(self, client) -> None:
        # The test environment (like development) runs without secrets.
        resp = await client.post(PATH, params=_params())
        assert resp.status_code == 200

    async def test_production_without_secrets_refuses(self, client, monkeypatch) -> None:
        monkeypatch.setattr(get_settings(), "app_env", "production")
        resp = await client.post(PATH, params=_params())
        assert resp.status_code == 503

    async def test_valid_signature_is_accepted(self, client, secrets_configured) -> None:
        body = json.dumps({"duration": 3}).encode()
        url, content, headers = _signed("secret-a", _params(), body)
        resp = await client.post(url, content=content, headers=headers)
        assert resp.status_code == 200, resp.text
        assert resp.json()["processed"] is True

    async def test_missing_signature_is_rejected(self, client, secrets_configured) -> None:
        assert (await client.post(PATH, params=_params())).status_code == 401

    async def test_wrong_secret_or_other_providers_secret_is_rejected(self, client, secrets_configured) -> None:
        for secret in ("guess", "secret-b"):  # provider B cannot sign provider A's events
            url, content, headers = _signed(secret, _params())
            assert (await client.post(url, content=content, headers=headers)).status_code == 401

    async def test_tampered_query_or_body_is_rejected(self, client, secrets_configured) -> None:
        url, content, headers = _signed("secret-a", _params(), b'{"a": 1}')
        tampered_url = url.replace("event_type=ringing", "event_type=completed")
        assert (await client.post(tampered_url, content=content, headers=headers)).status_code == 401
        assert (await client.post(url, content=b'{"a": 2}', headers=headers)).status_code == 401

    async def test_stale_timestamp_is_rejected(self, client, secrets_configured) -> None:
        url, content, headers = _signed("secret-a", _params(), ts=int(time.time()) - 3600)
        assert (await client.post(url, content=content, headers=headers)).status_code == 401

    async def test_unknown_provider_is_rejected(self, client, secrets_configured) -> None:
        url, content, headers = _signed("secret-a", _params(provider_id="someone_else"))
        assert (await client.post(url, content=content, headers=headers)).status_code == 401

    async def test_replay_within_window_changes_nothing(self, client, secrets_configured) -> None:
        url, content, headers = _signed("secret-a", _params(event_id="replayed"))
        first = await client.post(url, content=content, headers=headers)
        second = await client.post(url, content=content, headers=headers)
        assert first.status_code == second.status_code == 200
        assert second.json()["state_changed"] is False  # idempotent inbox
