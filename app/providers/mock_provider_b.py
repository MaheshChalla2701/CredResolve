"""Mock Provider B — slow, unreliable, duplicate/out-of-order events.

Characteristics:
- Higher latency (100–800ms)
- Intermittent timeouts (~10%)
- Duplicate event delivery
- Out-of-order event sequences
- Occasional delayed events (up to 5s extra delay)
- Configurable failure bursts
- Does NOT guarantee event order
"""
from __future__ import annotations

import asyncio
import random
import uuid
from datetime import datetime, timezone
from typing import Any

from app.providers.base import (
    ProviderAdapter,
    ProviderCallRequest,
    ProviderCallResponse,
    ProviderEvent,
    ProviderHealthStatus,
)
from app.core.logging import get_logger

logger = get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class MockProviderB(ProviderAdapter):
    """Unreliable, high-latency mock provider with problematic event delivery."""

    PROVIDER_ID = "mock_provider_b"

    def __init__(
        self,
        *,
        seed: int | None = None,
        success_rate: float = 0.80,
        timeout_rate: float = 0.10,
        min_latency_ms: float = 100.0,
        max_latency_ms: float = 800.0,
        answer_rate: float = 0.55,
        avg_talk_seconds: float = 90.0,
        talk_stddev: float = 30.0,
        duplicate_event_prob: float = 0.25,  # probability each event is duplicated
        out_of_order_prob: float = 0.15,     # probability events arrive out of order
        delayed_event_prob: float = 0.20,    # probability of an extra delay
        max_extra_delay_seconds: float = 5.0,
        failure_burst_size: int = 0,         # 0 = no burst; N = fail N consecutive
        event_queue: asyncio.Queue | None = None,
        simulate_outage: bool = False,
    ) -> None:
        self._rng = random.Random(seed)
        self.success_rate = success_rate
        self.timeout_rate = timeout_rate
        self.min_latency_ms = min_latency_ms
        self.max_latency_ms = max_latency_ms
        self.answer_rate = answer_rate
        self.avg_talk_seconds = avg_talk_seconds
        self.talk_stddev = talk_stddev
        self.duplicate_event_prob = duplicate_event_prob
        self.out_of_order_prob = out_of_order_prob
        self.delayed_event_prob = delayed_event_prob
        self.max_extra_delay_seconds = max_extra_delay_seconds
        self.simulate_outage = simulate_outage

        self._consecutive_failures = 0
        self._failure_burst_remaining = failure_burst_size

        self._idempotency: dict[str, str] = {}
        self._active_calls: dict[str, dict] = {}

        self._event_queue: asyncio.Queue[ProviderEvent] = (
            event_queue if event_queue is not None else asyncio.Queue()
        )

        self._total = 0
        self._failures = 0

    @property
    def provider_id(self) -> str:
        return self.PROVIDER_ID

    @property
    def event_queue(self) -> asyncio.Queue[ProviderEvent]:
        return self._event_queue

    def trigger_failure_burst(self, count: int) -> None:
        self._failure_burst_remaining = count

    async def initiate_call(
        self, request: ProviderCallRequest
    ) -> ProviderCallResponse:
        self._total += 1

        if self.simulate_outage:
            self._failures += 1
            return ProviderCallResponse(
                accepted=False,
                provider_call_id=None,
                error_code="PROVIDER_OUTAGE",
            )

        # Idempotency
        if request.idempotency_key in self._idempotency:
            return ProviderCallResponse(
                accepted=True,
                provider_call_id=self._idempotency[request.idempotency_key],
            )

        # Simulate network latency (higher than Provider A)
        latency = self._rng.uniform(self.min_latency_ms, self.max_latency_ms) / 1000
        await asyncio.sleep(latency)

        # Failure burst
        if self._failure_burst_remaining > 0:
            self._failure_burst_remaining -= 1
            self._failures += 1
            return ProviderCallResponse(
                accepted=False,
                provider_call_id=None,
                error_code="BURST_FAILURE",
            )

        # Timeout simulation
        if self._rng.random() < self.timeout_rate:
            self._failures += 1
            raise asyncio.TimeoutError("Provider B simulated timeout")

        # Random failure
        if self._rng.random() > self.success_rate:
            self._failures += 1
            return ProviderCallResponse(
                accepted=False,
                provider_call_id=None,
                error_code="PROVIDER_B_ERROR",
            )

        provider_call_id = f"pB-{uuid.uuid4().hex[:12]}"
        self._idempotency[request.idempotency_key] = provider_call_id
        self._active_calls[provider_call_id] = {"call_id": request.call_id}

        asyncio.create_task(
            self._deliver_call_events_chaotic(request.call_id, provider_call_id)
        )

        return ProviderCallResponse(accepted=True, provider_call_id=provider_call_id)

    async def _deliver_event_maybe_duplicate(
        self, event: ProviderEvent
    ) -> None:
        """Deliver an event, optionally with an extra delay and/or duplicate."""
        if self._rng.random() < self.delayed_event_prob:
            delay = self._rng.uniform(0.1, self.max_extra_delay_seconds)
            await asyncio.sleep(delay)

        await self._event_queue.put(event)

        # Optionally duplicate
        if self._rng.random() < self.duplicate_event_prob:
            await asyncio.sleep(self._rng.uniform(0.01, 0.5))
            await self._event_queue.put(event)
            logger.debug("provider_b_duplicate_event", event_id=event.provider_event_id)

    async def _deliver_call_events_chaotic(
        self, call_id: str, provider_call_id: str
    ) -> None:
        seq = [0]

        def _event(etype: str, **extra: Any) -> ProviderEvent:
            seq[0] += 1
            return ProviderEvent(
                provider_id=self.PROVIDER_ID,
                provider_event_id=f"{provider_call_id}-{seq[0]}",
                provider_call_id=provider_call_id,
                event_type=etype,
                sequence_number=seq[0],
                raw_payload={"call_id": call_id, **extra},
            )

        # Build event sequence
        events = []
        ringing_ev = _event("ringing")
        events.append((self._rng.uniform(0.5, 2.0), ringing_ev))

        if self._rng.random() < self.answer_rate:
            answered_ev = _event("answered")
            connected_ev = _event("connected")
            talk = max(1.0, self._rng.gauss(self.avg_talk_seconds, self.talk_stddev))
            completed_ev = _event("completed", duration_seconds=talk)

            events.append((events[-1][0] + self._rng.uniform(1.0, 4.0), answered_ev))
            events.append((events[-1][0] + self._rng.uniform(0.1, 0.5), connected_ev))
            events.append((events[-1][0] + min(talk, 5.0), completed_ev))
        else:
            outcome = self._rng.choice(["no_answer", "busy", "failed"])
            no_answer_ev = _event(outcome)
            events.append((events[-1][0] + self._rng.uniform(1.0, 3.0), no_answer_ev))

        # Optionally shuffle some events (out-of-order delivery)
        if self._rng.random() < self.out_of_order_prob and len(events) > 2:
            # Swap last two events
            events[-1], events[-2] = events[-2], events[-1]
            logger.debug("provider_b_out_of_order", provider_call_id=provider_call_id)

        # Deliver
        for delay, ev in events:
            await asyncio.sleep(delay / 10)  # scaled for simulation speed
            asyncio.create_task(self._deliver_event_maybe_duplicate(ev))

        self._active_calls.pop(provider_call_id, None)

    async def get_call_status(self, provider_call_id: str) -> ProviderEvent | None:
        if provider_call_id in self._active_calls:
            return ProviderEvent(
                provider_id=self.PROVIDER_ID,
                provider_event_id=f"{provider_call_id}-status",
                provider_call_id=provider_call_id,
                event_type="ringing",
            )
        return None

    async def cancel_call(self, provider_call_id: str) -> bool:
        if provider_call_id in self._active_calls:
            self._active_calls.pop(provider_call_id)
            await self._event_queue.put(
                ProviderEvent(
                    provider_id=self.PROVIDER_ID,
                    provider_event_id=f"{provider_call_id}-cancel",
                    provider_call_id=provider_call_id,
                    event_type="cancelled",
                )
            )
            return True
        return False

    async def health_check(self) -> ProviderHealthStatus:
        rate = 1.0 - (self._failures / max(1, self._total))
        return ProviderHealthStatus(
            provider_id=self.PROVIDER_ID,
            is_healthy=not self.simulate_outage and rate >= 0.7,
            success_rate_1min=rate,
            avg_latency_ms=(self.min_latency_ms + self.max_latency_ms) / 2,
            circuit_state="OPEN" if self.simulate_outage else "CLOSED",
        )

    def normalize_event(self, raw: dict[str, Any]) -> ProviderEvent:
        return ProviderEvent(
            provider_id=self.PROVIDER_ID,
            provider_event_id=raw["event_id"],
            provider_call_id=raw["call_id"],
            event_type=raw["event_type"],
            raw_payload=raw,
        )
