"""Unit tests for Mock Provider A and Mock Provider B."""
from __future__ import annotations

import asyncio
import pytest

from app.providers.base import ProviderCallRequest
from app.providers.mock_provider_a import MockProviderA
from app.providers.mock_provider_b import MockProviderB


@pytest.mark.asyncio
class TestMockProviderA:
    async def test_idempotent_initiate_call(self) -> None:
        """Calling initiate_call with the same idempotency_key returns identical provider_call_id."""
        provider = MockProviderA(seed=42, min_latency_ms=1.0, max_latency_ms=2.0)
        req = ProviderCallRequest(
            call_id="call-001",
            campaign_id="camp-001",
            masked_phone="+15551234567",
            idempotency_key="idemp-key-001",
        )

        resp1 = await provider.initiate_call(req)
        assert resp1.accepted is True
        assert resp1.provider_call_id is not None

        resp2 = await provider.initiate_call(req)
        assert resp2.accepted is True
        assert resp2.provider_call_id == resp1.provider_call_id

    async def test_outage_simulation(self) -> None:
        """When simulate_outage is True, initiate_call fails with error."""
        provider = MockProviderA(simulate_outage=True)
        req = ProviderCallRequest(
            call_id="call-002",
            campaign_id="camp-001",
            masked_phone="+15551234567",
            idempotency_key="idemp-key-002",
        )
        resp = await provider.initiate_call(req)
        assert resp.accepted is False
        assert resp.error_code == "PROVIDER_OUTAGE"


@pytest.mark.asyncio
class TestMockProviderB:
    async def test_duplicate_and_delay_configuration(self) -> None:
        """Provider B initializes with configured chaos settings."""
        provider = MockProviderB(
            seed=42,
            min_latency_ms=1.0,
            max_latency_ms=2.0,
            duplicate_event_prob=1.0,  # force duplicate events
        )
        assert provider.provider_id == "mock_provider_b"

    async def test_idempotent_call_initiation(self) -> None:
        """Provider B also guarantees idempotency on duplicate keys."""
        provider = MockProviderB(
            seed=100,
            min_latency_ms=1.0,
            max_latency_ms=2.0,
            success_rate=1.0,
            timeout_rate=0.0,
        )
        req = ProviderCallRequest(
            call_id="call-b-001",
            campaign_id="camp-001",
            masked_phone="+15559876543",
            idempotency_key="idemp-b-001",
        )
        resp1 = await provider.initiate_call(req)
        assert resp1.accepted is True

        resp2 = await provider.initiate_call(req)
        assert resp2.accepted is True
        assert resp2.provider_call_id == resp1.provider_call_id

    async def test_failure_burst(self) -> None:
        """Configured failure burst triggers consecutive rejections."""
        provider = MockProviderB(
            min_latency_ms=1.0,
            max_latency_ms=2.0,
            failure_burst_size=3,
        )
        for i in range(3):
            req = ProviderCallRequest(
                call_id=f"burst-call-{i}",
                campaign_id="camp-001",
                masked_phone="+15559876543",
                idempotency_key=f"idemp-burst-{i}",
            )
            resp = await provider.initiate_call(req)
            assert resp.accepted is False
            assert resp.error_code == "BURST_FAILURE"
