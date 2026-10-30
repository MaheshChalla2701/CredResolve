"""Simulation engine — runs controlled scenarios for both dialing modes.

Each scenario is a configurable parameter set. The engine:
  1. Creates a synthetic campaign, agents, and borrowers in the database.
  2. Starts the same DialerRuntime the API uses (dispatch worker, provider
     event consumer, reconciliation) against its own mock provider instance.
  3. Runs dialing cycles through the same run_dialing_cycle() as POST /dial,
     so both modes pass through the Safety Controller.
  4. Waits for an explicit completion condition — every call terminal, no
     dispatch in flight, no unmatched provider events — bounded by a timeout.
  5. Computes metrics from the database and checks invariants.

`completed` is False (and the run is not a success) if the timeout expires
with calls still unresolved. Fixed random seeds make provider behaviour
reproducible; wall-clock timing still varies slightly between runs.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from app.core.config import get_settings
from app.core.enums import (
    ACTIVE_CALL_STATES,
    TERMINAL_CALL_STATES,
    AgentState,
    BorrowerStatus,
    CallState,
    DispatchStatus,
)
from app.core.logging import get_logger
from app.persistence.models import (
    Agent,
    Borrower,
    Call,
    Campaign,
    DispatchRecord,
    ProviderEventInbox,
    SafetyDecisionRecord,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
_settings = get_settings()


SCENARIO_CONFIGS: dict[str, dict] = {
    "low_answer_rate": {
        "answer_rate": 0.20,
        "avg_talk_seconds": 120.0,
        "talk_stddev": 30.0,
        "num_cycles": 15,
        "cycle_sleep": 0.05,
    },
    "medium_answer_rate": {
        "answer_rate": 0.50,
        "avg_talk_seconds": 90.0,
        "talk_stddev": 20.0,
        "num_cycles": 15,
        "cycle_sleep": 0.05,
    },
    "high_answer_rate": {
        "answer_rate": 0.70,
        "avg_talk_seconds": 60.0,
        "talk_stddev": 15.0,
        "num_cycles": 15,
        "cycle_sleep": 0.05,
    },
    "variable_answer_rate": {
        "answer_rate": 0.50,
        "avg_talk_seconds": 90.0,
        "talk_stddev": 25.0,
        "num_cycles": 20,
        "cycle_sleep": 0.05,
        "answer_rate_change_at_cycle": 10,
        "answer_rate_after_change": 0.10,
    },
    "provider_degradation": {
        "answer_rate": 0.50,
        "avg_talk_seconds": 80.0,
        "talk_stddev": 20.0,
        "num_cycles": 15,
        "cycle_sleep": 0.05,
        "provider": "mock_provider_b",
        "provider_failure_burst_at_cycle": 5,
        "provider_failure_burst_size": 8,
    },
    "agent_churn": {
        "answer_rate": 0.50,
        "avg_talk_seconds": 90.0,
        "talk_stddev": 20.0,
        "num_cycles": 15,
        "cycle_sleep": 0.05,
        "agent_churn_at_cycle": 5,
        "agent_churn_fraction": 0.40,
    },
    "burst_answering": {
        "answer_rate": 0.80,
        "avg_talk_seconds": 30.0,
        "talk_stddev": 5.0,
        "num_cycles": 15,
        "cycle_sleep": 0.02,
    },
}

# Non-final dispatch statuses: the outbox still owes the provider a request/answer
_OPEN_DISPATCH_STATUSES = [DispatchStatus.PENDING, DispatchStatus.CLAIMED, DispatchStatus.SENT]


@dataclass
class SimulationMetrics:
    calls_proposed: int = 0
    calls_approved: int = 0
    calls_rejected: int = 0
    calls_reserved: int = 0
    calls_initiated: int = 0
    calls_answered: int = 0
    calls_connected: int = 0
    calls_completed: int = 0
    calls_failed: int = 0
    calls_cancelled: int = 0
    calls_abandoned: int = 0
    calls_unresolved: int = 0
    failure_reasons: dict[str, int] = field(default_factory=dict)
    safety_decisions: dict[str, int] = field(default_factory=lambda: {"APPROVE": 0, "REDUCE": 0, "REJECT": 0})
    pacing_decisions: int = 0
    idle_cycles: int = 0
    invariant_violations: list[str] = field(default_factory=list)
    cycle_times_ms: list[float] = field(default_factory=list)
    agent_utilization_pct: float = 0.0
    abandonment_rate_pct: float = 0.0


class SimulationEngine:
    """Runs a single simulation scenario."""

    def __init__(
        self,
        *,
        scenario_name: str = "medium_answer_rate",
        mode: str = "progressive",
        seed: int = 42,
        num_agents: int = 20,
        num_borrowers: int = 100,
        db_session: "AsyncSession | None" = None,
        delay_scale: float = 1.0,
        drain_timeout_seconds: float = 60.0,
        maintenance_interval: float = 0.25,
    ) -> None:
        self.scenario_name = scenario_name
        self.mode = mode
        self.seed = seed
        self.num_agents = num_agents
        self.num_borrowers = num_borrowers
        self._session = db_session  # kept for API compatibility; each step uses its own session
        self.delay_scale = delay_scale
        self.drain_timeout_seconds = drain_timeout_seconds
        self.maintenance_interval = maintenance_interval
        self.config = {**SCENARIO_CONFIGS.get(scenario_name, SCENARIO_CONFIGS["medium_answer_rate"])}
        self.metrics = SimulationMetrics()
        self._talking_samples = 0
        self._staffed_samples = 0

    def _build_provider(self):
        provider_name = self.config.get("provider", "mock_provider_a")
        kwargs = dict(
            seed=self.seed,
            answer_rate=self.config["answer_rate"],
            avg_talk_seconds=self.config["avg_talk_seconds"],
            talk_stddev=self.config["talk_stddev"],
            delay_scale=self.delay_scale,
            # A private provider id keeps simulation calls away from the
            # API's own dispatch workers when run via /simulation/run.
            provider_id=f"sim-{provider_name}-{uuid.uuid4().hex[:6]}",
        )
        if provider_name == "mock_provider_b":
            from app.providers.mock_provider_b import MockProviderB
            return MockProviderB(**kwargs)
        from app.providers.mock_provider_a import MockProviderA
        return MockProviderA(**kwargs)

    async def run(self) -> dict[str, Any]:
        """Execute the simulation. Returns the metrics report."""
        from app.persistence.database import AsyncSessionLocal
        from app.services.dialing import run_dialing_cycle
        from app.services.reconciliation import ReconciliationService
        from app.workers.runtime import DialerRuntime

        session_factory = AsyncSessionLocal
        logger.info(
            "simulation_start",
            scenario=self.scenario_name,
            mode=self.mode,
            seed=self.seed,
            agents=self.num_agents,
            borrowers=self.num_borrowers,
        )

        provider = self._build_provider()
        provider_id = provider.provider_id
        runtime = DialerRuntime(
            {provider_id: provider},
            session_factory,
            dispatch_interval=0.05,
            maintenance_interval=self.maintenance_interval,
            reconciler=ReconciliationService(
                provider_adapters={provider_id: provider},
                ambiguous_grace_seconds=1.0,
            ),
        )
        campaign_id = await self._create_fixtures(session_factory)

        started = time.perf_counter()
        await runtime.start()
        try:
            for cycle in range(self.config.get("num_cycles", 15)):
                await self._apply_scenario_mutations(cycle, campaign_id, session_factory, provider)
                t0 = time.perf_counter()
                async with session_factory() as session:
                    campaign = await session.get(Campaign, campaign_id)
                    result = await run_dialing_cycle(
                        session,
                        campaign,
                        provider_id=provider_id,
                        safety=runtime.safety_controller(),
                        health_service=runtime.health[provider_id],
                    )
                    await session.commit()
                self._record_cycle(result)
                if result.calls:
                    runtime.notify_dispatch()
                self.metrics.cycle_times_ms.append((time.perf_counter() - t0) * 1000)
                await self._sample_agents(campaign_id, session_factory)
                await asyncio.sleep(self.config.get("cycle_sleep", 0.05))

            completed = await self._wait_for_completion(campaign_id, session_factory, provider_id)
        finally:
            await runtime.stop()

        await self._collect_final_metrics(campaign_id, session_factory)
        await self._check_invariants(campaign_id, session_factory)

        report = self._build_report(campaign_id, completed, time.perf_counter() - started)
        logger.info(
            "simulation_complete",
            scenario=self.scenario_name,
            mode=self.mode,
            completed=completed,
            unresolved=self.metrics.calls_unresolved,
            violations=len(self.metrics.invariant_violations),
        )
        return report

    def _record_cycle(self, result) -> None:
        decision = result.decision
        self.metrics.calls_proposed += decision.requested_count
        self.metrics.calls_approved += len(result.calls)
        if result.recommendation is not None:
            self.metrics.pacing_decisions += 1
        if decision.requested_count == 0:
            self.metrics.idle_cycles += 1  # nothing proposed; safety not consulted
        else:
            self.metrics.calls_rejected += decision.requested_count - decision.approved_count

    async def _wait_for_completion(self, campaign_id: str, session_factory, provider_id: str) -> bool:
        """Wait until every call is terminal, no dispatch is in flight and no provider
        event is still waiting to be matched — or until the drain timeout."""
        deadline = time.perf_counter() + self.drain_timeout_seconds
        while True:
            async with session_factory() as session:
                open_calls = (await session.execute(
                    select(func.count(Call.id)).where(
                        Call.campaign_id == campaign_id,
                        Call.state.not_in(list(TERMINAL_CALL_STATES)),
                    )
                )).scalar_one()
                open_dispatch = (await session.execute(
                    select(func.count(DispatchRecord.id)).where(
                        DispatchRecord.campaign_id == campaign_id,
                        DispatchRecord.status.in_(_OPEN_DISPATCH_STATUSES),
                    )
                )).scalar_one()
                unmatched = (await session.execute(
                    select(func.count(ProviderEventInbox.id)).where(
                        ProviderEventInbox.provider_id == provider_id,
                        ProviderEventInbox.processing_status == "PENDING",
                    )
                )).scalar_one()
            await self._sample_agents(campaign_id, session_factory)
            if open_calls == 0 and open_dispatch == 0 and unmatched == 0:
                return True
            if time.perf_counter() >= deadline:
                logger.warning(
                    "simulation_drain_timeout",
                    open_calls=open_calls,
                    open_dispatch=open_dispatch,
                    unmatched_events=unmatched,
                )
                return False
            await asyncio.sleep(0.1)

    async def _sample_agents(self, campaign_id: str, session_factory) -> None:
        """Talk-time utilisation sample: CONNECTED agents / staffed agents
        (OFFLINE and PAUSED agents are not staffed)."""
        async with session_factory() as session:
            rows = (await session.execute(
                select(Agent.state, func.count(Agent.id))
                .where(Agent.campaign_id == campaign_id)
                .group_by(Agent.state)
            )).all()
        counts = {state: n for state, n in rows}
        staffed = sum(counts.values()) - counts.get(AgentState.OFFLINE, 0) - counts.get(AgentState.PAUSED, 0)
        self._staffed_samples += staffed
        self._talking_samples += counts.get(AgentState.CONNECTED, 0)

    async def _create_fixtures(self, session_factory) -> str:
        async with session_factory() as session:
            from app.persistence.repositories import CampaignRepository, AgentRepository, BorrowerRepository
            from faker import Faker
            fake = Faker()
            fake.seed_instance(self.seed)

            campaign = Campaign(
                name=f"SIM-{self.scenario_name}-{self.mode}-{uuid.uuid4().hex[:6]}",
                status="ACTIVE",
                dialing_mode=self.mode,
                max_concurrent_calls=self.num_agents * 3,
                max_attempts_per_borrower=3,
                retry_cooldown_seconds=1,  # short for simulation
                wrap_up_seconds=1,
            )
            camp_repo = CampaignRepository(session)
            campaign = await camp_repo.create(campaign)

            agent_repo = AgentRepository(session)
            for i in range(self.num_agents):
                agent = Agent(
                    campaign_id=campaign.id,
                    name=f"Agent-{i:03d}",
                    state=AgentState.AVAILABLE,
                )
                await agent_repo.create(agent)

            borrower_repo = BorrowerRepository(session)
            for i in range(self.num_borrowers):
                borrower = Borrower(
                    campaign_id=campaign.id,
                    synthetic_name=fake.name(),
                    masked_phone=f"***-***-{i:04d}",
                    priority=i % 10,
                    status="QUEUED",
                    max_attempts=3,
                )
                await borrower_repo.create(borrower)

            await session.commit()
            self._campaign_id = campaign.id
            return campaign.id

    async def _apply_scenario_mutations(
        self, cycle: int, campaign_id: str, session_factory, provider
    ) -> None:
        """Apply scenario-specific mutations at the right cycle."""
        # Variable answer rate change
        change_at = self.config.get("answer_rate_change_at_cycle")
        if change_at and cycle == change_at:
            new_rate = self.config.get("answer_rate_after_change", 0.10)
            provider.answer_rate = new_rate
            logger.info(
                "scenario_mutation_answer_rate_change",
                cycle=cycle,
                new_rate=new_rate,
            )

        # Provider failure burst
        burst_at = self.config.get("provider_failure_burst_at_cycle")
        if burst_at and cycle == burst_at and hasattr(provider, "trigger_failure_burst"):
            burst_size = self.config.get("provider_failure_burst_size", 5)
            provider.trigger_failure_burst(burst_size)
            logger.info(
                "scenario_mutation_provider_burst",
                cycle=cycle,
                size=burst_size,
            )

        # Agent churn: a fraction of AVAILABLE agents log off (validated transition)
        churn_at = self.config.get("agent_churn_at_cycle")
        if churn_at and cycle == churn_at:
            frac = self.config.get("agent_churn_fraction", 0.40)
            async with session_factory() as session:
                from app.persistence.repositories import AgentRepository
                repo = AgentRepository(session)
                counts = await repo.count_by_state(campaign_id)
                churn_n = int(counts.get(AgentState.AVAILABLE, 0) * frac)
                offlined = 0
                for ag in await repo.list_available(campaign_id, limit=churn_n) if churn_n else []:
                    if await repo.transition_state(
                        ag.id, from_state=AgentState.AVAILABLE, to_state=AgentState.OFFLINE, version=ag.version
                    ):
                        offlined += 1
                await session.commit()
            logger.info("scenario_mutation_agent_churn", cycle=cycle, offlined=offlined)

    async def _collect_final_metrics(self, campaign_id: str, session_factory) -> None:
        m = self.metrics
        async with session_factory() as session:
            calls = (await session.execute(
                select(Call.state, Call.provider_call_id, Call.answered_at, Call.connected_at, Call.failure_reason)
                .where(Call.campaign_id == campaign_id)
            )).all()
            decisions = (await session.execute(
                select(SafetyDecisionRecord.decision, func.count(SafetyDecisionRecord.id))
                .where(SafetyDecisionRecord.campaign_id == campaign_id)
                .group_by(SafetyDecisionRecord.decision)
            )).all()

        reasons: Counter[str] = Counter()
        for state, provider_call_id, answered_at, connected_at, failure_reason in calls:
            answered = answered_at is not None or connected_at is not None or state == CallState.COMPLETED
            m.calls_reserved += 1
            m.calls_initiated += provider_call_id is not None
            m.calls_answered += answered
            m.calls_connected += connected_at is not None
            m.calls_completed += state == CallState.COMPLETED
            m.calls_failed += state == CallState.FAILED
            m.calls_cancelled += state == CallState.CANCELLED
            m.calls_unresolved += state not in TERMINAL_CALL_STATES
            # Answered by the borrower but never bridged to an agent
            m.calls_abandoned += answered and connected_at is None and state in TERMINAL_CALL_STATES
            if state == CallState.FAILED:
                reasons[failure_reason or "unknown"] += 1
        m.failure_reasons = dict(reasons)
        m.safety_decisions = {"APPROVE": 0, "REDUCE": 0, "REJECT": 0}
        for decision, n in decisions:
            m.safety_decisions[decision] = n
        m.abandonment_rate_pct = round(m.calls_abandoned / m.calls_answered * 100, 2) if m.calls_answered else 0.0
        m.agent_utilization_pct = (
            round(self._talking_samples / self._staffed_samples * 100, 1) if self._staffed_samples else 0.0
        )

    async def _check_invariants(self, campaign_id: str, session_factory) -> None:
        """Run invariant checks. Records violations in metrics."""
        violations = self.metrics.invariant_violations
        async with session_factory() as session:
            # Invariant 1: No agent has >1 active call
            double_assigned = (await session.execute(
                select(Call.agent_id, func.count(Call.id))
                .where(Call.campaign_id == campaign_id, Call.state.in_(list(ACTIVE_CALL_STATES)))
                .group_by(Call.agent_id)
                .having(func.count(Call.id) > 1)
            )).all()
            for agent_id, n in double_assigned:
                violations.append(f"AGENT_DOUBLE_ASSIGNED: agent={agent_id} active_calls={n}")

            # Invariant 2: No agent held by a finished (or missing) call
            leaked = (await session.execute(
                select(Agent.id, Agent.state, Call.state)
                .outerjoin(Call, Agent.current_call_id == Call.id)
                .where(
                    Agent.campaign_id == campaign_id,
                    Agent.state.in_([AgentState.RESERVED, AgentState.DIALING, AgentState.CONNECTED]),
                )
            )).all()
            for agent_id, agent_state, call_state in leaked:
                if call_state is None or call_state in TERMINAL_CALL_STATES:
                    violations.append(f"AGENT_LEAKED: agent={agent_id} state={agent_state} call_state={call_state}")

            # Invariant 3: No borrower ALLOCATED without an active call
            stuck = (await session.execute(
                select(Borrower.id).where(
                    Borrower.campaign_id == campaign_id,
                    Borrower.status == BorrowerStatus.ALLOCATED,
                    ~Borrower.id.in_(
                        select(Call.borrower_id).where(
                            Call.campaign_id == campaign_id,
                            Call.state.in_(list(ACTIVE_CALL_STATES)),
                        )
                    ),
                )
            )).scalars().all()
            for borrower_id in stuck:
                violations.append(f"BORROWER_STUCK_ALLOCATED: borrower={borrower_id}")

        for v in violations:
            logger.error("invariant_violation", violation=v)

    def _build_report(self, campaign_id: str, completed: bool, elapsed_s: float) -> dict:
        import statistics
        m = self.metrics
        cycle_times = m.cycle_times_ms
        return {
            "scenario": self.scenario_name,
            "mode": self.mode,
            "seed": self.seed,
            "num_agents": self.num_agents,
            "num_borrowers": self.num_borrowers,
            "campaign_id": campaign_id,
            # Success requires: drained before the timeout, nothing unresolved,
            # no invariant violations.
            "completed": completed,
            "success": completed and m.calls_unresolved == 0 and not m.invariant_violations,
            "elapsed_seconds": round(elapsed_s, 2),
            "metrics": {
                "calls_proposed": m.calls_proposed,
                "calls_approved": m.calls_approved,
                "calls_rejected": m.calls_rejected,
                "calls_reserved": m.calls_reserved,
                "calls_initiated": m.calls_initiated,
                "calls_answered": m.calls_answered,
                "calls_connected": m.calls_connected,
                "calls_completed": m.calls_completed,
                "calls_failed": m.calls_failed,
                "calls_cancelled": m.calls_cancelled,
                "calls_abandoned": m.calls_abandoned,
                "calls_unresolved": m.calls_unresolved,
                "failure_reasons": m.failure_reasons,
                "abandonment_rate_pct": m.abandonment_rate_pct,
                "agent_utilization_pct": m.agent_utilization_pct,
                "safety_decisions": m.safety_decisions,
                "pacing_decisions": m.pacing_decisions,
                "idle_cycles": m.idle_cycles,
                "invariant_violations": m.invariant_violations,
                "cycle_time_p50_ms": round(statistics.median(cycle_times), 2) if cycle_times else 0,
                "cycle_time_p95_ms": round(
                    sorted(cycle_times)[int(len(cycle_times) * 0.95)] if len(cycle_times) >= 20 else max(cycle_times or [0]), 2
                ),
            },
        }
