"""In-process background runtime: dispatch workers, provider event consumers and
periodic reconciliation.

Started and stopped by the FastAPI lifespan (app/main.py). It runs:
  * a dispatch loop — one DispatchWorker per registered provider drains the
    dispatch outbox; woken immediately after /dial, otherwise polls;
  * one event-consumer loop per provider — provider events → EventProcessor,
    each event in its own transaction, retried a bounded number of times;
  * a maintenance loop — re-applies events that arrived before their
    provider_call_id was recorded, then runs reconciliation (wrap-up release,
    stale reservations, ambiguous dispatches, leaked agents).

Each loop iteration is also exposed as a method (dispatch_once, drain_events,
maintenance_once) so tests and the simulation drive exactly the same code.

Trade-off: running the workers inside the API process keeps the prototype to a
single command, and lets the in-memory mock providers deliver their events to
the process that dispatched the call. In production the dispatch worker would
be its own process (the outbox + lease design already allows several workers),
and real providers would deliver events to POST /api/v1/provider-events.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Mapping

from fastapi import Request

from app.core.config import get_settings
from app.core.logging import get_logger
from app.core.timeutils import as_utc, utcnow
from app.persistence.database import AsyncSessionLocal
from app.persistence.repositories import ProviderEventRepository
from app.services.event_processor import EventProcessor
from app.services.provider_health import ProviderHealthService
from app.services.reconciliation import ReconciliationService
from app.services.safety_controller import SafetyController
from app.workers.dispatch_worker import DispatchWorker

if TYPE_CHECKING:
    from app.providers.base import ProviderAdapter, ProviderEvent

logger = get_logger(__name__)


class DialerRuntime:
    def __init__(
        self,
        providers: Mapping[str, "ProviderAdapter"],
        session_factory: Any = None,
        *,
        dispatch_interval: float | None = None,
        maintenance_interval: float | None = None,
        event_max_retries: int | None = None,
        reconciler: ReconciliationService | None = None,
        batch_size: int = 20,
    ) -> None:
        settings = get_settings()
        self.providers: dict[str, ProviderAdapter] = dict(providers)
        self.session_factory = session_factory or AsyncSessionLocal
        self.dispatch_interval = dispatch_interval or settings.dispatch_poll_interval_seconds
        self.maintenance_interval = maintenance_interval or settings.reconciliation_interval_seconds
        self.event_max_retries = event_max_retries or settings.event_max_retries
        self.unmatched_event_max_age = timedelta(seconds=settings.unmatched_event_max_age_seconds)
        self.batch_size = batch_size

        self.health = {pid: ProviderHealthService(pid) for pid in self.providers}
        self.workers = {
            pid: DispatchWorker(
                provider,
                worker_id=f"{pid}-{uuid.uuid4().hex[:6]}",
                health_service=self.health[pid],
            )
            for pid, provider in self.providers.items()
        }
        self.event_processor = EventProcessor()
        self.reconciler = reconciler or ReconciliationService(provider_adapters=self.providers)

        self._wakeup = asyncio.Event()
        self._stopping = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self.stats = {"dispatched": 0, "events_processed": 0, "event_failures": 0}

    @classmethod
    def with_mock_providers(cls, **kwargs: Any) -> "DialerRuntime":
        """Runtime wired to the two mock providers (no real telephony)."""
        from app.providers.mock_provider_a import MockProviderA
        from app.providers.mock_provider_b import MockProviderB

        settings = get_settings()
        seed = settings.mock_provider_seed
        scale = settings.mock_provider_delay_scale
        providers = {
            MockProviderA.PROVIDER_ID: MockProviderA(seed=seed, delay_scale=scale),
            MockProviderB.PROVIDER_ID: MockProviderB(seed=seed, delay_scale=scale),
        }
        return cls(providers, **kwargs)

    # ── Wiring helpers ────────────────────────────────────────────────────
    @property
    def running(self) -> bool:
        return any(not t.done() for t in self._tasks)

    def safety_controller(self) -> SafetyController:
        """Safety Controller wired to this runtime's provider circuit breakers."""
        return SafetyController(provider_health_services=dict(self.health))

    def notify_dispatch(self) -> None:
        """Wake the dispatch loop (new outbox records were committed)."""
        self._wakeup.set()

    # ── Lifecycle ─────────────────────────────────────────────────────────
    async def start(self) -> None:
        if self.running:
            return
        self._stopping.clear()
        self._tasks = [asyncio.create_task(self._dispatch_loop(), name="dispatch-loop")]
        for pid, provider in self.providers.items():
            self._tasks.append(
                asyncio.create_task(self._event_loop(provider), name=f"events-{pid}")
            )
        self._tasks.append(asyncio.create_task(self._maintenance_loop(), name="maintenance-loop"))
        logger.info("dialer_runtime_started", providers=list(self.providers))

    async def stop(self, *, grace_seconds: float = 10.0) -> None:
        """Stop the loops gracefully: each finishes its current unit of work (so no
        database transaction is interrupted mid-flight), then exits. Anything
        still running after grace_seconds is cancelled."""
        tasks, self._tasks = self._tasks, []
        self._stopping.set()
        self._wakeup.set()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=grace_seconds)
            for task in pending:
                logger.warning("dialer_runtime_task_cancelled", task=task.get_name())
                task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results):
            if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                logger.error("dialer_runtime_task_failed", task=task.get_name(), error=repr(result))
        for provider in self.providers.values():
            await provider.aclose()
        if tasks:
            logger.info("dialer_runtime_stopped", stats=self.stats)

    # ── Single steps ──────────────────────────────────────────────────────
    async def dispatch_once(self) -> int:
        """Run every provider's dispatch worker once. Returns records claimed."""
        total = 0
        for worker in self.workers.values():
            total += await worker.run_once(self.session_factory, batch_size=self.batch_size)
        self.stats["dispatched"] += total
        return total

    async def process_event(self, event: "ProviderEvent") -> bool:
        """Process one provider event in its own transaction."""
        async with self.session_factory() as session:
            changed = await self.event_processor.process_raw(
                session,
                provider_id=event.provider_id,
                provider_event_id=event.provider_event_id,
                provider_call_id=event.provider_call_id,
                event_type=event.event_type,
                payload=event.raw_payload,
                sequence_number=event.sequence_number,
            )
            await session.commit()
        self.stats["events_processed"] += 1
        return changed

    async def drain_events(self) -> int:
        """Process every event currently queued at the providers (no waiting)."""
        handled = 0
        for provider in self.providers.values():
            queue = provider.event_queue
            while not queue.empty():
                await self._handle_event(queue.get_nowait())
                handled += 1
        return handled

    async def retry_unmatched_events(self) -> int:
        """Re-apply inbox events that arrived before their provider_call_id was recorded."""
        async with self.session_factory() as session:
            pending = list(await ProviderEventRepository(session).list_pending(limit=200))
        give_up_before = utcnow() - self.unmatched_event_max_age
        applied = 0
        for entry in pending:
            try:
                async with self.session_factory() as session:
                    changed = await self.event_processor.retry_unmatched(
                        session,
                        inbox_id=entry.id,
                        provider_id=entry.provider_id,
                        provider_event_id=entry.provider_event_id,
                        provider_call_id=entry.provider_call_id,
                        event_type=entry.event_type,
                        give_up=as_utc(entry.received_at) < give_up_before,
                    )
                    await session.commit()
                applied += int(changed)
            except Exception:
                logger.exception("unmatched_event_retry_failed", inbox_id=entry.id)
        return applied

    async def reconcile_once(self) -> dict:
        async with self.session_factory() as session:
            result = await self.reconciler.run(session)
            await session.commit()
        return result

    async def maintenance_once(self) -> dict:
        retried = await self.retry_unmatched_events()
        result = await self.reconcile_once()
        result["unmatched_events_applied"] = retried
        return result

    # ── Loops ─────────────────────────────────────────────────────────────
    async def _handle_event(self, event: "ProviderEvent") -> None:
        for attempt in range(1, self.event_max_retries + 1):
            try:
                await self.process_event(event)
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "event_processing_error",
                    provider_event_id=event.provider_event_id,
                    event_type=event.event_type,
                    attempt=attempt,
                )
                await asyncio.sleep(0.1 * attempt)
        # The failed transaction rolled back its inbox row, so a redelivery would
        # be processed; the stale-call probe in reconciliation covers lost outcomes.
        self.stats["event_failures"] += 1
        logger.error(
            "event_processing_gave_up",
            provider_event_id=event.provider_event_id,
            provider_call_id=event.provider_call_id,
            event_type=event.event_type,
        )

    @staticmethod
    async def _idle(event: asyncio.Event, timeout: float) -> None:
        """Wait until event is set or timeout elapses (the only cancellation-safe
        waiting points in the loops)."""
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    async def _dispatch_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                claimed = await self.dispatch_once()
            except Exception:
                logger.exception("dispatch_loop_error")
                claimed = 0
            if claimed == 0 and not self._stopping.is_set():
                await self._idle(self._wakeup, self.dispatch_interval)
                self._wakeup.clear()

    async def _event_loop(self, provider: "ProviderAdapter") -> None:
        queue = provider.event_queue
        while not self._stopping.is_set():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue
            await self._handle_event(event)

    async def _maintenance_loop(self) -> None:
        while not self._stopping.is_set():
            await self._idle(self._stopping, self.maintenance_interval)
            if self._stopping.is_set():
                break
            try:
                await self.maintenance_once()
            except Exception:
                logger.exception("maintenance_loop_error")


def get_runtime(request: Request) -> DialerRuntime:
    """FastAPI dependency: the app's runtime (created without background loops if
    the lifespan did not run, e.g. under a bare ASGI test transport)."""
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        runtime = DialerRuntime.with_mock_providers()
        request.app.state.runtime = runtime
    return runtime
