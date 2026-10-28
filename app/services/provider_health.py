"""Provider health tracking and circuit breaker.

Circuit breaker states:
  CLOSED   — normal operation, all requests proceed
  OPEN     — all requests blocked; a cooldown must elapse before recovery
  HALF_OPEN — limited recovery probes allowed; success closes, failure reopens

The health record is stored in PostgreSQL so that multiple workers share state.
For performance, a short in-memory cache avoids hitting the DB on every request,
but safety decisions re-read from the DB before approving allocations.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from app.core.config import get_settings
from app.core.enums import CircuitBreakerState
from app.core.logging import get_logger
from app.core.metrics import circuit_breaker_state as cb_metric, provider_requests

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
_settings = get_settings()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ProviderHealthService:
    """Manages circuit breaker state for a single provider.

    The service reads/writes ProviderHealthRecord rows and exposes
    a check_and_record method to be called before and after provider
    requests.

    Concurrency note: DB row-level locking is used when transitioning
    circuit states to avoid races between workers.
    """

    # In-memory cache TTL — for quick hot-path checks only
    _CACHE_TTL_SECONDS = 5.0

    def __init__(self, provider_id: str) -> None:
        self.provider_id = provider_id
        self._cached_state: str = CircuitBreakerState.CLOSED
        self._cache_updated_at: datetime = datetime.min.replace(tzinfo=timezone.utc)
        self._lock = asyncio.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    async def is_available(self, session: "AsyncSession") -> bool:
        """Fast check: is the circuit closed or half-open (allowing probes)?"""
        await self._refresh_cache(session)
        return self._cached_state in (
            CircuitBreakerState.CLOSED,
            CircuitBreakerState.HALF_OPEN,
        )

    async def record_success(self, session: "AsyncSession") -> None:
        from app.persistence.repositories import ProviderHealthRepository
        repo = ProviderHealthRepository(session)
        record = await repo.get_for_update(self.provider_id)

        record.total_requests += 1
        record.consecutive_failures = 0
        record.last_success_at = _utcnow()

        if record.circuit_state == CircuitBreakerState.HALF_OPEN:
            record.half_open_probes_sent += 1
            if record.half_open_probes_sent >= _settings.circuit_breaker_half_open_probes:
                record.circuit_state = CircuitBreakerState.CLOSED
                record.circuit_opened_at = None
                record.half_open_probes_sent = 0
                logger.info("circuit_breaker_closed", provider=self.provider_id)

        record.updated_at = _utcnow()
        await repo.save(record)
        self._update_cache(record.circuit_state)
        provider_requests.labels(provider=self.provider_id, outcome="success").inc()
        cb_metric.labels(provider=self.provider_id).set(
            self._state_to_int(record.circuit_state)
        )

    async def record_failure(self, session: "AsyncSession", *, is_timeout: bool = False) -> None:
        from app.persistence.repositories import ProviderHealthRepository
        repo = ProviderHealthRepository(session)
        record = await repo.get_for_update(self.provider_id)

        record.total_requests += 1
        record.total_failures += 1
        record.consecutive_failures += 1
        record.last_failure_at = _utcnow()

        if record.circuit_state == CircuitBreakerState.HALF_OPEN:
            # Any failure in HALF_OPEN → reopen
            record.circuit_state = CircuitBreakerState.OPEN
            record.circuit_opened_at = _utcnow()
            record.half_open_probes_sent = 0
            logger.warning("circuit_breaker_reopened", provider=self.provider_id)

        elif (
            record.circuit_state == CircuitBreakerState.CLOSED
            and record.consecutive_failures >= _settings.circuit_breaker_failure_threshold
        ):
            record.circuit_state = CircuitBreakerState.OPEN
            record.circuit_opened_at = _utcnow()
            logger.warning(
                "circuit_breaker_opened",
                provider=self.provider_id,
                consecutive=record.consecutive_failures,
            )

        record.updated_at = _utcnow()
        await repo.save(record)
        self._update_cache(record.circuit_state)
        label = "timeout" if is_timeout else "failure"
        provider_requests.labels(provider=self.provider_id, outcome=label).inc()
        cb_metric.labels(provider=self.provider_id).set(
            self._state_to_int(record.circuit_state)
        )

    async def maybe_transition_to_half_open(self, session: "AsyncSession") -> bool:
        """If OPEN and cooldown has elapsed, transition to HALF_OPEN. Returns True if transitioned."""
        from app.persistence.repositories import ProviderHealthRepository
        repo = ProviderHealthRepository(session)
        record = await repo.get_for_update(self.provider_id)

        if record.circuit_state != CircuitBreakerState.OPEN:
            return False

        cooldown = timedelta(seconds=_settings.circuit_breaker_cooldown_seconds)
        if record.circuit_opened_at and (_utcnow() - record.circuit_opened_at) >= cooldown:
            record.circuit_state = CircuitBreakerState.HALF_OPEN
            record.half_open_probes_sent = 0
            record.updated_at = _utcnow()
            await repo.save(record)
            self._update_cache(CircuitBreakerState.HALF_OPEN)
            logger.info("circuit_breaker_half_open", provider=self.provider_id)
            cb_metric.labels(provider=self.provider_id).set(2)
            return True
        return False

    async def get_snapshot(self, session: "AsyncSession") -> dict:
        from app.persistence.repositories import ProviderHealthRepository
        repo = ProviderHealthRepository(session)
        record = await repo.get_or_create(self.provider_id)
        total = max(1, record.total_requests)
        return {
            "provider_id": self.provider_id,
            "circuit_state": record.circuit_state,
            "consecutive_failures": record.consecutive_failures,
            "success_rate": round(1.0 - record.total_failures / total, 4),
            "last_failure_at": record.last_failure_at.isoformat() if record.last_failure_at else None,
            "last_success_at": record.last_success_at.isoformat() if record.last_success_at else None,
        }

    # ── Internals ────────────────────────────────────────────────────────────

    async def _refresh_cache(self, session: "AsyncSession") -> None:
        now = _utcnow()
        if (now - self._cache_updated_at).total_seconds() < self._CACHE_TTL_SECONDS:
            return
        async with self._lock:
            if (now - self._cache_updated_at).total_seconds() < self._CACHE_TTL_SECONDS:
                return
            from app.persistence.repositories import ProviderHealthRepository
            repo = ProviderHealthRepository(session)
            record = await repo.get_or_create(self.provider_id)
            self._update_cache(record.circuit_state)
            # Also attempt transition to HALF_OPEN if cooldown elapsed
            if record.circuit_state == CircuitBreakerState.OPEN:
                await self.maybe_transition_to_half_open(session)

    def _update_cache(self, state: str) -> None:
        self._cached_state = state
        self._cache_updated_at = _utcnow()

    @staticmethod
    def _state_to_int(state: str) -> int:
        return {
            CircuitBreakerState.CLOSED: 0,
            CircuitBreakerState.OPEN: 1,
            CircuitBreakerState.HALF_OPEN: 2,
        }.get(state, 0)
