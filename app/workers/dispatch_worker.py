"""Dispatch worker — the only component that calls provider.initiate_call().

Workflow for each dispatch record:
  1. Claim a PENDING dispatch record with a lease (UPDATE ... WHERE status=PENDING
     AND (lease_expires_at IS NULL OR lease_expires_at < NOW())).
  2. Update call state: RESERVED → INITIATED.
  3. Call provider.initiate_call() OUTSIDE any database transaction.
  4. Persist the provider response (provider_call_id or error).
  5. Update dispatch record status.

Crash recovery:
  - If the worker crashes after step 3 but before step 4, the lease will
    expire. Reconciliation will find the dispatch record in CLAIMED state
    with an expired lease.
  - Since we cannot know whether the provider accepted the call, we must NOT
    automatically retry. Instead, we mark the call as AMBIGUOUS and the
    dispatch record as AMBIGUOUS.
  - Reconciliation can then probe the provider via get_call_status() and
    resolve the ambiguity.

Idempotency:
  - The same idempotency_key is sent to the provider on every attempt.
  - Provider A replays the same provider_call_id for a known key.
  - Provider B also idempotent.
  - If provider returns the same provider_call_id, it's safe.

Why we never hold a DB transaction during the provider call:
  - Holding a transaction open during an external HTTP call can exhaust
    the connection pool and cause lock escalation.
  - We commit the lease claim, then call the provider, then commit the result.
  - The lease prevents another worker from re-claiming during our window.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from app.core.config import get_settings
from app.core.enums import CallState, DispatchStatus
from app.core.logging import get_logger
from app.core.metrics import calls_initiated

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from app.providers.base import ProviderAdapter

logger = get_logger(__name__)
_settings = get_settings()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DispatchWorker:
    """Reads the dispatch outbox and calls the provider."""

    def __init__(
        self,
        provider: "ProviderAdapter",
        worker_id: str | None = None,
    ) -> None:
        self._provider = provider
        self._worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"

    async def run_once(
        self,
        session_factory,
        *,
        batch_size: int = 10,
    ) -> int:
        """Process up to batch_size dispatch records. Returns count processed."""

        # ── Claim records (commit the lease immediately) ───────────────────
        async with session_factory() as session:
            from app.persistence.repositories import DispatchRepository
            dispatch_repo = DispatchRepository(session)
            records = await dispatch_repo.claim_pending(
                self._worker_id,
                lease_seconds=_settings.dispatch_lease_seconds,
                batch_size=batch_size,
            )
            await session.commit()

        if not records:
            return 0

        processed = 0
        for record in records:
            await self._dispatch_one(session_factory, record)
            processed += 1

        return processed

    async def _dispatch_one(self, session_factory, record) -> None:
        """Dispatch a single record. Commits results outside any DB transaction."""
        from app.providers.base import ProviderCallRequest

        call_id = record.call_id

        # ── Step 1: Transition call RESERVED → INITIATED ──────────────────
        async with session_factory() as session:
            from app.persistence.repositories import CallRepository, DispatchRepository
            call_repo = CallRepository(session)
            call = await call_repo.get_for_update(call_id)
            if call is None:
                logger.error("dispatch_call_not_found", call_id=call_id)
                return

            if call.state != CallState.RESERVED:
                # Call may have been cancelled or already dispatched by another worker
                logger.warning(
                    "dispatch_call_not_reserved",
                    call_id=call_id,
                    state=call.state,
                )
                # Mark dispatch as failed
                dispatch_repo = DispatchRepository(session)
                await dispatch_repo.update_result(
                    record.id, status=DispatchStatus.FAILED,
                    result_code="CALL_NOT_RESERVED"
                )
                await session.commit()
                return

            # Transition to INITIATED
            updated = await call_repo.transition_state(
                call_id,
                from_state=CallState.RESERVED,
                to_state=CallState.INITIATED,
                version=call.state_version,
                initiated_at=_utcnow(),
            )
            if not updated:
                logger.warning("dispatch_transition_stale", call_id=call_id)
                await session.commit()
                return

            await session.commit()

        # ── Step 2: Call provider OUTSIDE DB transaction ──────────────────
        request = ProviderCallRequest(
            call_id=call_id,
            campaign_id=record.campaign_id,
            masked_phone=record.masked_phone,
            idempotency_key=record.idempotency_key,
        )

        provider_call_id: str | None = None
        dispatch_status = DispatchStatus.AMBIGUOUS
        result_code: str | None = None
        result_detail: dict | None = None
        is_ambiguous = False

        try:
            t0 = time.perf_counter()
            response = await self._provider.initiate_call(request)
            latency_ms = (time.perf_counter() - t0) * 1000

            if response.accepted and response.provider_call_id:
                provider_call_id = response.provider_call_id
                dispatch_status = DispatchStatus.SUCCEEDED
                result_code = "ACCEPTED"
                calls_initiated.labels(
                    campaign_id=record.campaign_id,
                    provider=self._provider.provider_id,
                ).inc()
                logger.info(
                    "call_dispatched",
                    call_id=call_id,
                    provider_call_id=provider_call_id,
                    latency_ms=round(latency_ms, 1),
                )
            else:
                dispatch_status = DispatchStatus.FAILED
                result_code = response.error_code or "REJECTED"
                result_detail = {"detail": response.error_detail}
                logger.warning(
                    "call_dispatch_rejected",
                    call_id=call_id,
                    error=response.error_code,
                )

        except asyncio.TimeoutError:
            # AMBIGUOUS: provider may have accepted the call.
            # We MUST NOT retry blindly.
            is_ambiguous = True
            dispatch_status = DispatchStatus.AMBIGUOUS
            result_code = "TIMEOUT"
            logger.error(
                "call_dispatch_timeout_ambiguous",
                call_id=call_id,
                worker=self._worker_id,
            )

        except Exception as exc:
            # Non-timeout exceptions are more likely definite failures,
            # but we treat as ambiguous to be safe.
            is_ambiguous = True
            dispatch_status = DispatchStatus.AMBIGUOUS
            result_code = f"ERROR:{type(exc).__name__}"
            logger.error(
                "call_dispatch_error",
                call_id=call_id,
                error=str(exc),
            )

        # ── Step 3: Persist result ────────────────────────────────────────
        async with session_factory() as session:
            from app.persistence.repositories import CallRepository, DispatchRepository
            call_repo = CallRepository(session)
            dispatch_repo = DispatchRepository(session)

            # Update dispatch record
            await dispatch_repo.update_result(
                record.id,
                status=dispatch_status,
                provider_call_id=provider_call_id,
                result_code=result_code,
                result_detail=result_detail or {},
            )

            if dispatch_status == DispatchStatus.SUCCEEDED and provider_call_id:
                # Persist provider_call_id on the call record
                await session.execute(
                    __import__("sqlalchemy", fromlist=["update"]).update(
                        __import__("app.persistence.models", fromlist=["Call"]).Call
                    )
                    .where(
                        __import__("app.persistence.models", fromlist=["Call"]).Call.id == call_id
                    )
                    .values(
                        provider_call_id=provider_call_id,
                        updated_at=_utcnow(),
                    )
                )

            elif dispatch_status == DispatchStatus.FAILED:
                # Definite failure — transition call to FAILED, release agent
                call = await call_repo.get_for_update(call_id)
                if call and call.state == CallState.INITIATED:
                    await call_repo.transition_state(
                        call_id,
                        from_state=CallState.INITIATED,
                        to_state=CallState.FAILED,
                        version=call.state_version,
                        failure_reason=result_code,
                        completed_at=_utcnow(),
                    )
                    # Release agent back to AVAILABLE
                    from app.persistence.repositories import AgentRepository
                    agent_repo = AgentRepository(session)
                    agent = await agent_repo.get_for_update(call.agent_id)
                    if agent:
                        await agent_repo.transition_state(
                            agent.id,
                            from_state=agent.state,
                            to_state="AVAILABLE",
                            version=agent.version,
                            clear_call=True,
                        )

            elif is_ambiguous:
                # Mark call as ambiguous — do NOT release agent yet
                from sqlalchemy import update
                from app.persistence.models import Call as CallModel
                await session.execute(
                    update(CallModel)
                    .where(CallModel.id == call_id)
                    .values(is_ambiguous=True, updated_at=_utcnow())
                )

            await session.commit()
