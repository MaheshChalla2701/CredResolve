"""Dispatch worker — the only component that calls provider.initiate_call().

Workflow for each dispatch record:
  1. Claim PENDING (or expired-lease CLAIMED) records for this worker's provider
     with a lease (compare-and-set, see DispatchRepository.claim_pending).
  2. In one transaction: fence the claim (CLAIMED-by-me → SENT) and move the
     call RESERVED → INITIATED. Committed BEFORE the provider is contacted.
  3. Call provider.initiate_call() OUTSIDE any database transaction.
  4. Persist the outcome:
       accepted  → dispatch SUCCEEDED, call.provider_call_id recorded
       rejected  → dispatch FAILED, call FAILED, agent released, borrower retried
       timeout / unexpected error → dispatch AMBIGUOUS, call.is_ambiguous = True
     and record provider health (circuit breaker input).

Duplicate-call safety:
  - Step 2 is the fence. A record can only be re-claimed while it is still
    CLAIMED, which proves the provider was never contacted for it. Once SENT it
    is never re-dispatched automatically.
  - If a worker dies after step 2, reconciliation finds the SENT record with an
    expired lease, marks the call ambiguous and resolves it by asking the
    provider whether it accepted the idempotency key.
  - The same idempotency_key is sent on every attempt; the mock providers
    return the original provider_call_id for a key they have already accepted.

Why we never hold a DB transaction during the provider call:
  - Holding a transaction open during an external call can exhaust the
    connection pool and hold row locks for the provider's latency.

Steps 2 and 4 lock rows in the global order call → dispatch → agent → borrower
→ provider health (app/persistence/retry.py) and are retried as a whole on a
deadlock or serialization failure.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import update

from app.core.config import get_settings
from app.core.enums import CallState, DispatchStatus
from app.core.errors import StaleUpdateError
from app.core.logging import get_logger
from app.core.metrics import calls_initiated
from app.core.timeutils import utcnow
from app.persistence.models import Call
from app.persistence.repositories import CallRepository, DispatchRepository
from app.persistence.retry import run_transaction
from app.services.call_outcomes import finish_call

if TYPE_CHECKING:
    from app.providers.base import ProviderAdapter
    from app.services.provider_health import ProviderHealthService

logger = get_logger(__name__)
_settings = get_settings()


class DispatchWorker:
    """Reads the dispatch outbox for one provider and calls that provider."""

    def __init__(
        self,
        provider: "ProviderAdapter",
        worker_id: str | None = None,
        *,
        health_service: "ProviderHealthService | None" = None,
    ) -> None:
        self._provider = provider
        self._worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._health = health_service

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def run_once(
        self,
        session_factory,
        *,
        batch_size: int = 10,
    ) -> int:
        """Claim and process up to batch_size dispatch records. Returns count claimed."""
        async with session_factory() as session:
            records = await DispatchRepository(session).claim_pending(
                self._worker_id,
                lease_seconds=_settings.dispatch_lease_seconds,
                batch_size=batch_size,
                provider_id=self._provider.provider_id,
            )
            await session.commit()

        for record in records:
            try:
                await self._dispatch_one(session_factory, record)
            except Exception:
                # Not swallowed silently: logged with traceback. The record is left
                # CLAIMED (re-claimed after its lease expires — the provider was not
                # contacted) or SENT (reconciliation resolves it as ambiguous).
                logger.exception(
                    "dispatch_record_error",
                    dispatch_id=record.id,
                    call_id=record.call_id,
                    worker=self._worker_id,
                )
        return len(records)

    async def _dispatch_one(self, session_factory, record) -> None:
        from app.providers.base import ProviderCallRequest

        call_id = record.call_id

        # ── Step 1: fence the claim and move RESERVED → INITIATED ─────────
        async def fence(session) -> bool:
            call_repo = CallRepository(session)
            dispatch_repo = DispatchRepository(session)
            call = await call_repo.get_for_update(call_id)
            if call is None:
                logger.error("dispatch_call_not_found", call_id=call_id)
                await dispatch_repo.update_result(
                    record.id, status=DispatchStatus.FAILED, result_code="CALL_NOT_FOUND"
                )
                return False

            if call.state != CallState.RESERVED:
                await self._settle_not_reserved(session, record, call)
                return False

            if not await dispatch_repo.mark_sent(record.id, self._worker_id):
                # Nothing was written; committing the empty transaction is harmless.
                logger.warning("dispatch_claim_lost", dispatch_id=record.id, worker=self._worker_id)
                return False

            updated = await call_repo.transition_state(
                call_id,
                from_state=CallState.RESERVED,
                to_state=CallState.INITIATED,
                version=call.state_version,
                initiated_at=utcnow(),
            )
            if not updated:
                raise StaleUpdateError(f"Call {call_id} changed while locked for dispatch")
            return True

        async with session_factory() as session:
            if not await run_transaction(session, fence, op="dispatch_fence"):
                return

        # ── Step 2: call the provider OUTSIDE any DB transaction ──────────
        request = ProviderCallRequest(
            call_id=call_id,
            campaign_id=record.campaign_id,
            masked_phone=record.masked_phone,
            idempotency_key=record.idempotency_key,
        )
        provider_call_id: str | None = None
        result_code: str | None = None
        result_detail: dict = {}
        is_timeout = False

        t0 = time.perf_counter()
        try:
            response = await self._provider.initiate_call(request)
            if response.accepted and response.provider_call_id:
                provider_call_id = response.provider_call_id
                dispatch_status = DispatchStatus.SUCCEEDED
                result_code = "ACCEPTED"
            else:
                dispatch_status = DispatchStatus.FAILED
                result_code = response.error_code or "REJECTED"
                result_detail = {"detail": response.error_detail}
        except asyncio.TimeoutError:
            # The provider may have accepted the call: must NOT retry blindly.
            dispatch_status = DispatchStatus.AMBIGUOUS
            result_code = "TIMEOUT"
            is_timeout = True
        except Exception as exc:
            # Unknown failure mode — treat as ambiguous (provider may have acted).
            logger.exception("call_dispatch_error", call_id=call_id)
            dispatch_status = DispatchStatus.AMBIGUOUS
            result_code = f"ERROR:{type(exc).__name__}"
            result_detail = {"detail": str(exc)}
        latency_ms = round((time.perf_counter() - t0) * 1000, 1)

        # ── Step 3: persist the outcome ───────────────────────────────────
        async def record_outcome(session) -> None:
            call_repo = CallRepository(session)
            # Lock order: call before dispatch record (same as step 1).
            call = await call_repo.get_for_update(call_id)
            await DispatchRepository(session).update_result(
                record.id,
                status=dispatch_status,
                provider_call_id=provider_call_id,
                result_code=result_code,
                result_detail=result_detail,
            )

            if dispatch_status == DispatchStatus.SUCCEEDED:
                await session.execute(
                    update(Call)
                    .where(Call.id == call_id)
                    .values(provider_call_id=provider_call_id, is_ambiguous=False, updated_at=utcnow())
                )
            elif dispatch_status == DispatchStatus.FAILED:
                # Definite rejection — the provider did not place the call.
                if call is not None and call.state == CallState.INITIATED:
                    await finish_call(session, call, to_state=CallState.FAILED, reason=result_code)
            else:
                await session.execute(
                    update(Call)
                    .where(Call.id == call_id)
                    .values(is_ambiguous=True, updated_at=utcnow())
                )

            if self._health is not None:
                if dispatch_status == DispatchStatus.SUCCEEDED:
                    await self._health.record_success(session)
                else:
                    await self._health.record_failure(session, is_timeout=is_timeout)

        async with session_factory() as session:
            await run_transaction(session, record_outcome, op="dispatch_outcome")

        if dispatch_status == DispatchStatus.SUCCEEDED:
            calls_initiated.labels(
                campaign_id=record.campaign_id, provider=self._provider.provider_id
            ).inc()
            logger.info(
                "call_dispatched",
                call_id=call_id,
                provider_call_id=provider_call_id,
                latency_ms=latency_ms,
            )
        elif dispatch_status == DispatchStatus.FAILED:
            logger.warning("call_dispatch_rejected", call_id=call_id, error=result_code)
        else:
            logger.error(
                "call_dispatch_ambiguous",
                call_id=call_id,
                result_code=result_code,
                worker=self._worker_id,
            )

    async def _settle_not_reserved(self, session, record, call) -> None:
        """The claimed record's call is no longer RESERVED; record why, never re-send."""
        dispatch_repo = DispatchRepository(session)
        if call.provider_call_id is not None:
            # Already dispatched (e.g. resolved by reconciliation).
            await dispatch_repo.update_result(
                record.id,
                status=DispatchStatus.SUCCEEDED,
                provider_call_id=call.provider_call_id,
                result_code="ALREADY_DISPATCHED",
            )
        elif call.state == CallState.INITIATED:
            # A previous attempt reached the provider stage with an unknown outcome.
            await dispatch_repo.update_result(
                record.id, status=DispatchStatus.AMBIGUOUS, result_code="PRIOR_ATTEMPT_UNKNOWN"
            )
            await session.execute(
                update(Call).where(Call.id == call.id).values(is_ambiguous=True, updated_at=utcnow())
            )
        else:
            # e.g. failed by reconciliation before it was ever sent.
            await dispatch_repo.update_result(
                record.id, status=DispatchStatus.FAILED, result_code="CALL_NOT_RESERVED"
            )
        logger.warning(
            "dispatch_call_not_reserved",
            call_id=call.id,
            state=call.state,
            dispatch_id=record.id,
        )
