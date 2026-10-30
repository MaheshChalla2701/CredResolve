"""Shared-secret (HMAC-SHA256) authentication for provider webhooks.

Each provider has its own secret (PROVIDER_WEBHOOK_SECRETS, a JSON object
{"provider_id": "secret"}). The provider signs every request:

    X-SmartDialer-Timestamp: <unix seconds>
    X-SmartDialer-Signature: v1=<hex HMAC-SHA256(secret, signed_message)>

    signed_message = "<timestamp>\\n<METHOD>\\n<path>\\n<raw query string>\\n" + raw body

The query string carries provider_id, provider_call_id and event_type, so they
are covered by the signature; the secret is looked up by provider_id, so one
provider cannot sign events for another.

Policy:
  * secrets configured        → every request must carry a valid signature for
                                its provider_id, with a timestamp within
                                PROVIDER_WEBHOOK_MAX_SKEW_SECONDS (else 401);
  * no secrets, production    → webhooks refused (503): fail closed;
  * no secrets, development / test → accepted unauthenticated (a warning is
                                logged at startup). The in-process mock
                                providers do not use the webhook at all.

Not covered here: transport security (terminate TLS in front of the API),
secret rotation (deploy the new secret to both sides together), and replays
inside the time window — those are absorbed by the idempotent event inbox
(unique provider_id + provider_event_id), so a replayed event changes nothing.
"""
from __future__ import annotations

import hashlib
import hmac
import time

from fastapi import HTTPException, Request

from app.core.config import Settings, get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

TIMESTAMP_HEADER = "X-SmartDialer-Timestamp"
SIGNATURE_HEADER = "X-SmartDialer-Signature"


def signed_message(timestamp: str, method: str, path: str, query: str, body: bytes) -> bytes:
    return f"{timestamp}\n{method.upper()}\n{path}\n{query}\n".encode() + body


def sign(secret: str, timestamp: str, method: str, path: str, query: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), signed_message(timestamp, method, path, query, body), hashlib.sha256)
    return f"v1={digest.hexdigest()}"


def webhook_auth_mode(settings: Settings) -> str:
    """'required', 'disabled' (dev/test without secrets) or 'refused' (production without secrets)."""
    if settings.provider_webhook_secrets:
        return "required"
    return "refused" if settings.app_env == "production" else "disabled"


async def verify_provider_webhook(request: Request, provider_id: str) -> None:
    """FastAPI-side check for POST /provider-events. Raises 401/503 on failure."""
    settings = get_settings()
    mode = webhook_auth_mode(settings)
    if mode == "disabled":
        return
    if mode == "refused":
        logger.error("provider_webhook_refused_no_secret", provider_id=provider_id)
        raise HTTPException(status_code=503, detail="Provider webhook authentication is not configured")

    secret = settings.provider_webhook_secrets.get(provider_id)
    timestamp = request.headers.get(TIMESTAMP_HEADER, "")
    signature = request.headers.get(SIGNATURE_HEADER, "")
    if not secret or not timestamp or not signature:
        _reject(provider_id, "missing secret, timestamp or signature")
    try:
        skew = abs(time.time() - int(timestamp))
    except ValueError:
        _reject(provider_id, "malformed timestamp")
    if skew > settings.provider_webhook_max_skew_seconds:
        _reject(provider_id, f"timestamp outside the {settings.provider_webhook_max_skew_seconds}s window")

    expected = sign(
        secret,
        timestamp,
        request.method,
        request.url.path,
        request.scope.get("query_string", b"").decode("latin-1"),
        await request.body(),
    )
    if not hmac.compare_digest(expected, signature):
        _reject(provider_id, "signature mismatch")


def _reject(provider_id: str, reason: str) -> None:
    logger.warning("provider_webhook_rejected", provider_id=provider_id, reason=reason)
    raise HTTPException(status_code=401, detail="Invalid provider webhook signature")
