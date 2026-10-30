"""Provider abstraction — base protocol and event data models."""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _uuid() -> str:
    return str(uuid.uuid4())


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ProviderCallRequest:
    call_id: str
    campaign_id: str
    masked_phone: str
    idempotency_key: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProviderCallResponse:
    accepted: bool
    provider_call_id: str | None
    error_code: str | None = None
    error_detail: str | None = None


@dataclass
class ProviderEvent:
    provider_id: str
    provider_event_id: str
    provider_call_id: str
    event_type: str  # maps to ProviderEventType enum value
    timestamp: datetime = field(default_factory=_utcnow)
    sequence_number: int | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    call_duration_seconds: float | None = None
    failure_code: str | None = None


@dataclass
class ProviderHealthStatus:
    provider_id: str
    is_healthy: bool
    success_rate_1min: float
    avg_latency_ms: float
    circuit_state: str
    detail: str = ""


class ProviderAdapter(ABC):
    """Interface that every provider adapter must implement.

    IMPORTANT: Provider adapters must NOT be imported or called by the
    predictive pacing engine or safety controller. Only the Call Allocator
    (dispatch worker) may call initiate_call.
    """

    @property
    @abstractmethod
    def provider_id(self) -> str:
        """Unique identifier for this provider."""

    @abstractmethod
    async def initiate_call(
        self, request: ProviderCallRequest
    ) -> ProviderCallResponse:
        """Initiate an outbound call. Must be idempotent given the same idempotency_key."""

    @abstractmethod
    async def get_call_status(self, provider_call_id: str) -> ProviderEvent | None:
        """Retrieve current status for a given provider call ID."""

    async def find_call_by_idempotency_key(self, idempotency_key: str) -> str | None:
        """Return the provider_call_id already created for this idempotency key, or
        None if the provider never accepted it. Reconciliation uses this to resolve
        dispatches whose outcome is unknown (e.g. a timeout). Adapters that cannot
        answer raise NotImplementedError and the call stays ambiguous."""
        raise NotImplementedError

    async def aclose(self) -> None:
        """Release background resources (called on application shutdown)."""
        return None

    @abstractmethod
    async def cancel_call(self, provider_call_id: str) -> bool:
        """Request cancellation. Returns True if cancellation was accepted."""

    @abstractmethod
    async def health_check(self) -> ProviderHealthStatus:
        """Check provider health."""

    @abstractmethod
    def normalize_event(self, raw: dict[str, Any]) -> ProviderEvent:
        """Normalize a raw provider webhook payload to a ProviderEvent."""
