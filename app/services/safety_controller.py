"""Independent Safety Controller.

This module is THE safety gate. It has one job: read fresh authoritative
database state and approve, reduce, or reject a proposed dial count.

CRITICAL BOUNDARIES:
1. This module MUST NOT import ProviderAdapter or any provider client.
2. The predictive engine's recommendation is advisory input only.
3. Every hard limit check re-reads from the database.
4. If database state is unavailable, we FAIL CLOSED (reject all).

Safety checks performed before approving each batch:
  1. Campaign must be ACTIVE.
  2. Proposed count must be positive.
  3. Active call count must not exceed campaign limit.
  4. Verified available agent capacity must exist.
  5. Provider circuit breaker must not be OPEN.
  6. In strict mode: every approved call needs a reserved agent slot.
  7. No approval if we cannot read fresh state.
  8. No approval that cannot be audited: if the decision record cannot be
     written, the request is rejected.

The decision is made under the campaign lock taken by the dialing cycle (see
CampaignRepository.lock_for_dialing), so concurrent cycles see each other's
allocations. After allocation the dialer calls record_allocation(), which
stores how many calls were actually reserved (capacity_snapshot
["allocated_count"]) and, if fewer than approved, an ALLOCATION_SHORTFALL reason.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from app.core.config import get_settings
from app.core.enums import ACTIVE_CALL_STATES, AgentState, CampaignStatus, SafetyDecision
from app.core.errors import CampaignNotActiveError, SafetyViolationError
from app.core.logging import get_logger
from app.core.metrics import safety_decisions as sd_metric

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
_settings = get_settings()


@dataclass
class SafetyDecisionResult:
    decision_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    campaign_id: str = ""
    requested_count: int = 0
    approved_count: int = 0
    decision: str = SafetyDecision.REJECT
    reason_codes: list[str] = field(default_factory=list)
    capacity_snapshot: dict = field(default_factory=dict)
    provider_health_snapshot: dict = field(default_factory=dict)
    decided_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Set by record_allocation(); None until the dialer has allocated.
    allocated_count: int | None = None
    # True once the decision is in the safety_decisions audit table.
    persisted: bool = False


class SafetyController:
    """Non-bypassable safety gate for all outbound call proposals.

    Instantiate once and reuse across requests (stateless checks).
    All state comes from fresh DB reads inside evaluate().
    """

    def __init__(
        self,
        *,
        strict_mode: bool | None = None,
        max_dial_ratio: float | None = None,
        provider_health_services: dict | None = None,
    ) -> None:
        self.strict_mode = strict_mode if strict_mode is not None else _settings.safety_strict_mode
        self.max_dial_ratio = max_dial_ratio if max_dial_ratio is not None else _settings.safety_max_dial_ratio
        # provider_id -> ProviderHealthService (injected — no provider clients)
        self._health: dict = provider_health_services or {}

    def register_health_service(self, provider_id: str, service: object) -> None:
        self._health[provider_id] = service

    async def evaluate(
        self,
        session: "AsyncSession",
        *,
        campaign_id: str,
        requested_count: int,
        provider_id: str,
    ) -> SafetyDecisionResult:
        """Evaluate a proposed batch of outbound calls.

        Returns a SafetyDecisionResult. Never raises — returns REJECT on any error.
        Persists the decision record to the database for auditability.
        """
        result = SafetyDecisionResult(
            campaign_id=campaign_id,
            requested_count=requested_count,
        )

        try:
            approved = await self._run_checks(
                session, result, campaign_id, requested_count, provider_id
            )
            result.approved_count = approved
            if approved == 0:
                result.decision = SafetyDecision.REJECT
            elif approved < requested_count:
                result.decision = SafetyDecision.REDUCE
            else:
                result.decision = SafetyDecision.APPROVE

        except CampaignNotActiveError as exc:
            result.approved_count = 0
            result.decision = SafetyDecision.REJECT
            result.reason_codes.append(f"CAMPAIGN_NOT_ACTIVE:{exc}")
        except Exception as exc:
            # FAIL CLOSED on any unexpected error
            logger.error(
                "safety_controller_error",
                campaign_id=campaign_id,
                error=str(exc),
                exc_info=True,
            )
            result.approved_count = 0
            result.decision = SafetyDecision.REJECT
            result.reason_codes.append(f"INTERNAL_ERROR:{type(exc).__name__}")

        # Persist decision record — an approval that cannot be audited is not given.
        try:
            await self._persist_decision(session, result)
        except Exception as exc:
            logger.error(
                "safety_decision_persist_failed",
                campaign_id=campaign_id,
                error=str(exc),
                exc_info=True,
            )
            result.approved_count = 0
            result.decision = SafetyDecision.REJECT
            result.reason_codes.append(f"AUDIT_PERSIST_FAILED:{type(exc).__name__}")

        # Update metrics
        sd_metric.labels(
            campaign_id=campaign_id, decision=result.decision
        ).inc()

        logger.info(
            "safety_decision",
            campaign_id=campaign_id,
            requested=requested_count,
            approved=result.approved_count,
            decision=result.decision,
            reasons=result.reason_codes,
        )
        return result

    async def _run_checks(
        self,
        session: "AsyncSession",
        result: SafetyDecisionResult,
        campaign_id: str,
        requested_count: int,
        provider_id: str,
    ) -> int:
        from app.persistence.repositories import (
            AgentRepository,
            CampaignRepository,
            CallRepository,
        )

        # ── Check 1: Basic validation ──────────────────────────────────────
        if requested_count <= 0:
            result.reason_codes.append("REQUESTED_COUNT_ZERO")
            return 0

        # ── Check 2: Campaign must be ACTIVE ──────────────────────────────
        campaign_repo = CampaignRepository(session)
        campaign = await campaign_repo.get(campaign_id)
        if campaign is None or campaign.status != CampaignStatus.ACTIVE:
            result.reason_codes.append("CAMPAIGN_NOT_ACTIVE")
            raise CampaignNotActiveError(campaign_id)

        # ── Check 3: Read fresh agent capacity ────────────────────────────
        agent_repo = AgentRepository(session)
        state_counts = await agent_repo.count_by_state(campaign_id)

        available_agents = state_counts.get(AgentState.AVAILABLE, 0)
        reserved_agents = state_counts.get(AgentState.RESERVED, 0)
        dialing_agents = state_counts.get(AgentState.DIALING, 0)
        connected_agents = state_counts.get(AgentState.CONNECTED, 0)
        wrap_up_agents = state_counts.get(AgentState.WRAP_UP, 0)

        total_staffed = (
            available_agents + reserved_agents + dialing_agents
            + connected_agents + wrap_up_agents
        )

        result.capacity_snapshot = {
            "available_agents": available_agents,
            "reserved_agents": reserved_agents,
            "dialing_agents": dialing_agents,
            "connected_agents": connected_agents,
            "wrap_up_agents": wrap_up_agents,
            "total_staffed": total_staffed,
        }

        # ── Check 4: Active call count ────────────────────────────────────
        call_repo = CallRepository(session)
        active_calls = await call_repo.count_active(campaign_id)
        result.capacity_snapshot["active_calls"] = active_calls

        # Campaign-level concurrency limit
        if active_calls >= campaign.max_concurrent_calls:
            result.reason_codes.append("CAMPAIGN_CONCURRENCY_LIMIT")
            return 0

        # ── Check 5: Provider circuit breaker ─────────────────────────────
        probe_limit: int | None = None
        health_service = self._health.get(provider_id)
        if health_service:
            health_snap = await health_service.get_snapshot(session)
            result.provider_health_snapshot = health_snap
            if health_snap.get("circuit_state") == "OPEN":
                result.reason_codes.append("CIRCUIT_BREAKER_OPEN")
                return 0
            if health_snap.get("circuit_state") == "HALF_OPEN":
                # Recovering provider: allow only a few probe calls.
                probe_limit = max(1, _settings.circuit_breaker_half_open_probes)
        else:
            result.provider_health_snapshot = {"provider_id": provider_id, "status": "unknown"}

        # ── Check 6: Compute approved count ───────────────────────────────
        # Headroom = max_concurrent_calls - active_calls
        concurrency_headroom = campaign.max_concurrent_calls - active_calls

        if self.strict_mode:
            # STRICT: every approved call needs a free agent slot.
            # An agent must be AVAILABLE to be reserved.
            # We do not count predicted future availability.
            max_by_agents = available_agents
        else:
            # PERMISSIVE: allow ratio-based over-dialing.
            # Cap at max_dial_ratio × available agents.
            already_in_flight = reserved_agents + dialing_agents
            capacity_slots = max(0, int(available_agents * self.max_dial_ratio) - already_in_flight)
            max_by_agents = capacity_slots

        if max_by_agents <= 0:
            result.reason_codes.append("NO_AVAILABLE_AGENT_CAPACITY")
            return 0

        approved = min(requested_count, concurrency_headroom, max_by_agents)
        if probe_limit is not None and approved > probe_limit:
            approved = probe_limit
            result.reason_codes.append("CIRCUIT_HALF_OPEN_PROBE_LIMIT")

        if approved <= 0:
            result.reason_codes.append("APPROVED_COUNT_ZERO_AFTER_LIMITS")
            return 0

        if approved < requested_count:
            result.reason_codes.append(f"REDUCED_FROM_{requested_count}_TO_{approved}")

        return approved

    async def record_allocation(
        self,
        session: "AsyncSession",
        result: SafetyDecisionResult,
        *,
        allocated: int,
        stop_reason: str | None = None,
    ) -> None:
        """Record on the decision how many calls were actually allocated.

        Allocation can fall short of the approval (an agent paused, or a
        borrower changed, after the decision). The approved count is left as
        decided; the shortfall and its cause are added to the audit record.
        """
        if allocated > result.approved_count:
            raise SafetyViolationError(
                f"{allocated} calls allocated against an approval of {result.approved_count} "
                f"(decision {result.decision_id})"
            )
        result.allocated_count = allocated
        if allocated < result.approved_count:
            result.reason_codes.append(f"ALLOCATION_SHORTFALL:{stop_reason or 'UNKNOWN'}")
            logger.warning(
                "safety_allocation_shortfall",
                campaign_id=result.campaign_id,
                decision_id=result.decision_id,
                approved=result.approved_count,
                allocated=allocated,
                reason=stop_reason,
            )
        if not result.persisted:
            return
        from app.persistence.models import SafetyDecisionRecord

        record = await session.get(SafetyDecisionRecord, result.decision_id)
        if record is None:
            raise SafetyViolationError(f"Safety decision {result.decision_id} is missing from the audit log")
        # JSON columns are not mutation-tracked: assign new objects.
        record.capacity_snapshot = {**(record.capacity_snapshot or {}), "allocated_count": allocated}
        record.reason_codes = list(result.reason_codes)
        await session.flush()

    async def _persist_decision(
        self, session: "AsyncSession | None", result: SafetyDecisionResult
    ) -> None:
        if session is None:
            return
        from app.persistence.models import SafetyDecisionRecord
        from app.persistence.repositories import SafetyDecisionRepository

        record = SafetyDecisionRecord(
            id=result.decision_id,
            campaign_id=result.campaign_id,
            requested_count=result.requested_count,
            approved_count=result.approved_count,
            decision=result.decision,
            reason_codes=list(result.reason_codes),
            capacity_snapshot=dict(result.capacity_snapshot),
            provider_health_snapshot=result.provider_health_snapshot,
            decided_at=result.decided_at,
        )
        await SafetyDecisionRepository(session).create(record)
        result.persisted = True
