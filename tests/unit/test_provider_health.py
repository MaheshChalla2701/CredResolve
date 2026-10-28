"""Unit tests for ProviderHealthService and Circuit Breaker."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.enums import CircuitBreakerState
from app.services.provider_health import ProviderHealthService

_settings = get_settings()


@pytest.mark.asyncio
class TestProviderHealthService:
    async def test_initial_circuit_is_closed(self, db_session: AsyncSession) -> None:
        service = ProviderHealthService("prov-test-01")
        assert await service.is_available(db_session) is True
        snapshot = await service.get_snapshot(db_session)
        assert snapshot["circuit_state"] == CircuitBreakerState.CLOSED

    async def test_failures_open_circuit(self, db_session: AsyncSession) -> None:
        service = ProviderHealthService("prov-test-02")
        threshold = _settings.circuit_breaker_failure_threshold

        for _ in range(threshold - 1):
            await service.record_failure(db_session)
            await db_session.commit()
            assert await service.is_available(db_session) is True

        # Next failure hits the threshold and trips the breaker
        await service.record_failure(db_session)
        await db_session.commit()

        snapshot = await service.get_snapshot(db_session)
        assert snapshot["circuit_state"] == CircuitBreakerState.OPEN
        assert await service.is_available(db_session) is False

    async def test_success_resets_failures(self, db_session: AsyncSession) -> None:
        service = ProviderHealthService("prov-test-03")
        threshold = _settings.circuit_breaker_failure_threshold

        for _ in range(threshold - 1):
            await service.record_failure(db_session)
            await db_session.commit()

        # Success resets failure count
        await service.record_success(db_session)
        await db_session.commit()

        snapshot = await service.get_snapshot(db_session)
        assert snapshot["consecutive_failures"] == 0
        assert snapshot["circuit_state"] == CircuitBreakerState.CLOSED

    async def test_half_open_recovery_and_reopen(self, db_session: AsyncSession) -> None:
        service = ProviderHealthService("prov-test-04")
        # Trip breaker
        for _ in range(_settings.circuit_breaker_failure_threshold):
            await service.record_failure(db_session)
        await db_session.commit()

        # Manually backdate circuit_opened_at past cooldown
        from app.persistence.repositories import ProviderHealthRepository
        repo = ProviderHealthRepository(db_session)
        record = await repo.get_for_update("prov-test-04")
        record.circuit_opened_at = datetime.now(timezone.utc) - timedelta(
            seconds=_settings.circuit_breaker_cooldown_seconds + 5
        )
        await repo.save(record)
        await db_session.commit()

        # Check cooldown transition to HALF_OPEN
        transitioned = await service.maybe_transition_to_half_open(db_session)
        await db_session.commit()
        assert transitioned is True

        snapshot = await service.get_snapshot(db_session)
        assert snapshot["circuit_state"] == CircuitBreakerState.HALF_OPEN

        # In HALF_OPEN, a failure immediately reopens
        await service.record_failure(db_session)
        await db_session.commit()
        snapshot2 = await service.get_snapshot(db_session)
        assert snapshot2["circuit_state"] == CircuitBreakerState.OPEN
