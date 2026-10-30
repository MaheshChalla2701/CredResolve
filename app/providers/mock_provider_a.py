"""Mock Provider A — fast, reliable, normally ordered events.

Characteristics:
- Low simulated latency (10–50ms)
- High success rate (95%)
- Few timeouts (<2%)
- Normally ordered events
- Unique event IDs
- Supports idempotent call initiation via idempotency_key
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


class MockProviderA(ProviderAdapter):
    """Reliable, low-latency mock provider.

    All state is in-memory (this is a mock). The idempotency map ensures that
    calling initiate_call twice with the same key returns the same provider_call_id.
    """

    PROVIDER_ID = "mock_provider_a"

    def __init__(
        self,
        *,
        seed: int | None = None,
        success_rate: float = 0.95,
        timeout_rate: float = 0.01,
        min_latency_ms: float = 10.0,
        max_latency_ms: float = 50.0,
        answer_rate: float = 0.60,
        avg_talk_seconds: float = 90.0,
        talk_stddev: float = 30.0,
        event_queue: asyncio.Queue | None = None,
        simulate_outage: bool = False,
        delay_scale: float = 1.0,
        provider_id: str | None = None,
    ) -> None:
        self._rng = random.Random(seed)
        # Multiplier on every simulated delay (latency and event timing)
        self.delay_scale = delay_scale
        self._provider_id = provider_id or self.PROVIDER_ID
        self.success_rate = success_rate
        self.timeout_rate = timeout_rate
        self.min_latency_ms = min_latency_ms
        self.max_latency_ms = max_latency_ms
        self.answer_rate = answer_rate
        self.avg_talk_seconds = avg_talk_seconds
        self.talk_stddev = talk_stddev
        self.simulate_outage = simulate_outage

        # Provider-side idempotency map: idempotency_key -> provider_call_id
        self._idempotency: dict[str, str] = {}
        # Active calls: provider_call_id -> metadata
        self._active_calls: dict[str, dict] = {}

        # Event queue for the event processor to consume
        self._event_queue: asyncio.Queue[ProviderEvent] = (
            event_queue if event_queue is not None else asyncio.Queue()
        )

        # Health tracking
        self._total = 0
        self._failures = 0

        # Event-delivery tasks, cancelled on aclose()
        self._background: set[asyncio.Task] = set()

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    async def aclose(self) -> None:
        tasks = list(self._background)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def event_queue(self) -> asyncio.Queue[ProviderEvent]:
        return self._event_queue

    async def initiate_call(
        self, request: ProviderCallRequest
    ) -> ProviderCallResponse:
        self._total += 1

        # Simulate outage
        if self.simulate_outage:
            self._failures += 1
            return ProviderCallResponse(
                accepted=False,
                provider_call_id=None,
                error_code="PROVIDER_OUTAGE",
                error_detail="Provider is in outage mode",
            )

        # Idempotency: same key → same provider_call_id
        if request.idempotency_key in self._idempotency:
            existing_pcid = self._idempotency[request.idempotency_key]
            logger.debug(
                "provider_a_idempotent_replay",
                idempotency_key=request.idempotency_key,
                provider_call_id=existing_pcid,
            )
            return ProviderCallResponse(accepted=True, provider_call_id=existing_pcid)

        # Simulate network latency
        latency = self._rng.uniform(self.min_latency_ms, self.max_latency_ms) / 1000
        await asyncio.sleep(latency * self.delay_scale)

        # Simulate timeout
        if self._rng.random() < self.timeout_rate:
            self._failures += 1
            raise asyncio.TimeoutError("Provider A simulated timeout")

        # Simulate failure
        if self._rng.random() > self.success_rate:
            self._failures += 1
            return ProviderCallResponse(
                accepted=False,
                provider_call_id=None,
                error_code="PROVIDER_REJECTED",
                error_detail="Provider rejected call",
            )

        # Success — assign a provider call ID
        provider_call_id = f"pA-{uuid.uuid4().hex[:12]}"
        self._idempotency[request.idempotency_key] = provider_call_id
        self._active_calls[provider_call_id] = {
            "call_id": request.call_id,
            "masked_phone": request.masked_phone,
            "started_at": _utcnow(),
        }

        # Schedule asynchronous events
        self._spawn(self._deliver_call_events(request.call_id, provider_call_id))

        return ProviderCallResponse(accepted=True, provider_call_id=provider_call_id)

    async def _deliver_call_events(
        self, call_id: str, provider_call_id: str
    ) -> None:
        """Deliver a sequence of events for a call — normally ordered."""
        seq = 0

        def _event(etype: str, **extra: Any) -> ProviderEvent:
            nonlocal seq
            seq += 1
            return ProviderEvent(
                provider_id=self._provider_id,
                provider_event_id=f"{provider_call_id}-{seq}",
                provider_call_id=provider_call_id,
                event_type=etype,
                sequence_number=seq,
                raw_payload={"call_id": call_id, **extra},
            )

        scale = self.delay_scale

        # Ringing event
        await asyncio.sleep(self._rng.uniform(0.5, 2.0) * scale)
        await self._event_queue.put(_event("ringing"))

        # Decide outcome
        if self._rng.random() < self.answer_rate:
            # Answered
            await asyncio.sleep(self._rng.uniform(1.0, 5.0) * scale)
            await self._event_queue.put(_event("answered"))

            # Connected
            await asyncio.sleep(self._rng.uniform(0.1, 0.5) * scale)
            await self._event_queue.put(_event("connected"))

            # Talk time
            talk = max(1.0, self._rng.gauss(self.avg_talk_seconds, self.talk_stddev))
            await asyncio.sleep(min(talk, 5.0) * scale)  # cap for simulation speed

            await self._event_queue.put(_event("completed", duration_seconds=talk))
        else:
            # No answer / busy
            outcome = self._rng.choice(["no_answer", "busy", "failed"])
            await asyncio.sleep(self._rng.uniform(10.0, 30.0) / 10 * scale)
            await self._event_queue.put(_event(outcome))

        self._active_calls.pop(provider_call_id, None)

    async def get_call_status(self, provider_call_id: str) -> ProviderEvent | None:
        if provider_call_id in self._active_calls:
            return ProviderEvent(
                provider_id=self._provider_id,
                provider_event_id=f"{provider_call_id}-status",
                provider_call_id=provider_call_id,
                event_type="ringing",
            )
        return None

    async def find_call_by_idempotency_key(self, idempotency_key: str) -> str | None:
        return self._idempotency.get(idempotency_key)

    async def cancel_call(self, provider_call_id: str) -> bool:
        if provider_call_id in self._active_calls:
            self._active_calls.pop(provider_call_id)
            await self._event_queue.put(
                ProviderEvent(
                    provider_id=self._provider_id,
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
            provider_id=self._provider_id,
            is_healthy=not self.simulate_outage and rate >= 0.8,
            success_rate_1min=rate,
            avg_latency_ms=(self.min_latency_ms + self.max_latency_ms) / 2,
            circuit_state="OPEN" if self.simulate_outage else "CLOSED",
        )

    def normalize_event(self, raw: dict[str, Any]) -> ProviderEvent:
        return ProviderEvent(
            provider_id=self._provider_id,
            provider_event_id=raw["event_id"],
            provider_call_id=raw["call_id"],
            event_type=raw["event_type"],
            raw_payload=raw,
        )
