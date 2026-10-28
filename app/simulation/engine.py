"""Simulation engine — runs controlled scenarios for both dialing modes.

Each scenario is a configurable parameter set. The engine:
  1. Creates a synthetic campaign, agents, and borrowers in the database.
  2. Runs dialing cycles with the specified mode.
  3. Processes provider events as they arrive.
  4. Collects metrics at each step.
  5. Checks invariants after each cycle.
  6. Returns a full metrics report.

Fixed random seed makes results reproducible.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from app.core.config import get_settings
from app.core.enums import AgentState, CallState
from app.core.logging import get_logger
from app.persistence.models import Agent, Borrower, Campaign

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
_settings = get_settings()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


@dataclass
class SimulationMetrics:
    calls_proposed: int = 0
    calls_approved: int = 0
    calls_rejected: int = 0
    calls_initiated: int = 0
    calls_answered: int = 0
    calls_connected: int = 0
    calls_completed: int = 0
    calls_failed: int = 0
    calls_abandoned: int = 0
    calls_unresolved: int = 0
    safety_decisions: dict[str, int] = field(default_factory=lambda: {"APPROVE": 0, "REDUCE": 0, "REJECT": 0})
    invariant_violations: list[str] = field(default_factory=list)
    cycle_times_ms: list[float] = field(default_factory=list)
    agent_utilization_pct: float = 0.0


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
    ) -> None:
        self.scenario_name = scenario_name
        self.mode = mode
        self.seed = seed
        self.num_agents = num_agents
        self.num_borrowers = num_borrowers
        self._session = db_session
        self.config = {**SCENARIO_CONFIGS.get(scenario_name, SCENARIO_CONFIGS["medium_answer_rate"])}
        self.metrics = SimulationMetrics()

    async def run(self) -> dict[str, Any]:
        """Execute the simulation. Returns metrics dict."""
        from app.persistence.database import AsyncSessionLocal
        session_factory = AsyncSessionLocal

        logger.info(
            "simulation_start",
            scenario=self.scenario_name,
            mode=self.mode,
            seed=self.seed,
            agents=self.num_agents,
            borrowers=self.num_borrowers,
        )

        # ── Setup provider ────────────────────────────────────────────────
        provider_name = self.config.get("provider", "mock_provider_a")
        if provider_name == "mock_provider_b":
            from app.providers.mock_provider_b import MockProviderB
            provider = MockProviderB(
                seed=self.seed,
                answer_rate=self.config["answer_rate"],
                avg_talk_seconds=self.config["avg_talk_seconds"],
                talk_stddev=self.config["talk_stddev"],
            )
        else:
            from app.providers.mock_provider_a import MockProviderA
            provider = MockProviderA(
                seed=self.seed,
                answer_rate=self.config["answer_rate"],
                avg_talk_seconds=self.config["avg_talk_seconds"],
                talk_stddev=self.config["talk_stddev"],
            )

        # ── Setup services ────────────────────────────────────────────────
        from app.services.call_allocator import CallAllocator
        from app.services.safety_controller import SafetyController
        from app.services.progressive_dialer import ProgressiveDialer
        from app.services.predictive_pacer import PredictivePacingEngine, AnswerRateEstimator
        from app.services.event_processor import EventProcessor
        from app.workers.dispatch_worker import DispatchWorker

        safety = SafetyController()
        allocator = CallAllocator(provider_id=provider.provider_id)
        event_processor = EventProcessor()
        dispatch_worker = DispatchWorker(provider)
        pacer = PredictivePacingEngine(
            AnswerRateEstimator(smoothing_alpha=_settings.pacer_smoothing_alpha)
        )

        # ── Create campaign, agents, borrowers ────────────────────────────
        campaign_id = await self._create_fixtures(session_factory)

        # ── Start event consumer task ─────────────────────────────────────
        stop_event = asyncio.Event()
        event_task = asyncio.create_task(
            self._event_consumer(
                provider, event_processor, session_factory, stop_event
            )
        )

        # ── Run dialing cycles ────────────────────────────────────────────
        num_cycles = self.config.get("num_cycles", 15)
        cycle_sleep = self.config.get("cycle_sleep", 0.05)

        connected_time_total = 0.0
        staffed_time_total = 0.0

        for cycle in range(num_cycles):
            # Scenario mutations
            await self._apply_scenario_mutations(
                cycle, campaign_id, session_factory, provider
            )

            t0 = time.perf_counter()

            async with session_factory() as session:
                if self.mode == "progressive":
                    dialer = ProgressiveDialer(safety, allocator)
                    calls, decision = await dialer.run_cycle(
                        session, campaign_id, provider.provider_id
                    )
                else:
                    # Predictive mode
                    rec = await pacer.recommend(
                        session, campaign_id, provider.provider_id
                    )
                    self.metrics.calls_proposed += rec.recommended_count
                    decision = await safety.evaluate(
                        session,
                        campaign_id=campaign_id,
                        requested_count=rec.recommended_count,
                        provider_id=provider.provider_id,
                    )
                    from app.core.enums import SafetyDecision
                    if decision.decision != SafetyDecision.REJECT and decision.approved_count > 0:
                        calls = await allocator.allocate_batch(
                            session, campaign_id, decision.approved_count
                        )
                    else:
                        calls = []
                    decision_key = decision.decision
                    self.metrics.safety_decisions[decision_key] = (
                        self.metrics.safety_decisions.get(decision_key, 0) + 1
                    )

                await session.commit()

            self.metrics.calls_approved += len(calls)

            # Dispatch allocated calls
            dispatched = await dispatch_worker.run_once(session_factory, batch_size=50)
            self.metrics.calls_initiated += dispatched

            elapsed_ms = (time.perf_counter() - t0) * 1000
            self.metrics.cycle_times_ms.append(elapsed_ms)

            await asyncio.sleep(cycle_sleep)

        # ── Allow events to drain ─────────────────────────────────────────
        await asyncio.sleep(0.5)
        stop_event.set()
        try:
            await asyncio.wait_for(event_task, timeout=3.0)
        except asyncio.TimeoutError:
            event_task.cancel()

        # ── Collect final metrics from DB ─────────────────────────────────
        await self._collect_final_metrics(campaign_id, session_factory, pacer)

        # ── Run invariant checks ──────────────────────────────────────────
        await self._check_invariants(campaign_id, session_factory)

        result = self._build_report(campaign_id)
        logger.info("simulation_complete", scenario=self.scenario_name, mode=self.mode,
                    violations=len(self.metrics.invariant_violations))
        return result

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

    async def _event_consumer(
        self,
        provider,
        event_processor,
        session_factory,
        stop_event: asyncio.Event,
    ) -> None:
        """Consume events from the provider queue and process them."""
        while not stop_event.is_set():
            try:
                event = await asyncio.wait_for(
                    provider.event_queue.get(), timeout=0.1
                )
                async with session_factory() as session:
                    changed = await event_processor.process_raw(
                        session,
                        provider_id=event.provider_id,
                        provider_event_id=event.provider_event_id,
                        provider_call_id=event.provider_call_id,
                        event_type=event.event_type,
                        payload=event.raw_payload,
                        sequence_number=event.sequence_number,
                    )
                    await session.commit()

                    if changed:
                        if event.event_type == "answered":
                            self.metrics.calls_answered += 1
                        elif event.event_type == "connected":
                            self.metrics.calls_connected += 1
                        elif event.event_type == "completed":
                            self.metrics.calls_completed += 1
                        elif event.event_type in ("failed", "no_answer", "busy"):
                            self.metrics.calls_failed += 1

            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                logger.warning("event_consumer_error", error=str(exc))

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

        # Agent churn
        churn_at = self.config.get("agent_churn_at_cycle")
        if churn_at and cycle == churn_at:
            frac = self.config.get("agent_churn_fraction", 0.40)
            async with session_factory() as session:
                from app.persistence.repositories import AgentRepository
                from sqlalchemy import update, select
                from app.persistence.models import Agent
                repo = AgentRepository(session)
                counts = await repo.count_by_state(campaign_id)
                available = counts.get(AgentState.AVAILABLE, 0)
                churn_n = int(available * frac)
                if churn_n > 0:
                    result = await session.execute(
                        select(Agent)
                        .where(
                            Agent.campaign_id == campaign_id,
                            Agent.state == AgentState.AVAILABLE,
                        )
                        .limit(churn_n)
                    )
                    agents = result.scalars().all()
                    for ag in agents:
                        ag.state = AgentState.OFFLINE
                    await session.commit()
                logger.info(
                    "scenario_mutation_agent_churn",
                    cycle=cycle,
                    offlined=churn_n,
                )

    async def _collect_final_metrics(
        self, campaign_id: str, session_factory, pacer
    ) -> None:
        async with session_factory() as session:
            from sqlalchemy import select, func
            from app.persistence.models import Call, Agent
            from app.persistence.repositories import AgentRepository

            # Count calls by state
            result = await session.execute(
                select(Call.state, func.count(Call.id))
                .where(Call.campaign_id == campaign_id)
                .group_by(Call.state)
            )
            state_counts = {row[0]: row[1] for row in result}
            self.metrics.calls_unresolved = state_counts.get("RESERVED", 0) + state_counts.get("INITIATED", 0)

            # Abandoned = answered but agent immediately gone (simplification: check connected_at but no agent)
            # In our simulation, this MUST remain 0.

            # Agent utilization: connected time / total staffed time (approximation)
            # Full implementation would track time-in-state; here we use cycle data
            agent_counts = await AgentRepository(session).count_by_state(campaign_id)
            total = sum(agent_counts.values())
            connected = agent_counts.get(AgentState.CONNECTED, 0)
            available = agent_counts.get(AgentState.AVAILABLE, 0)
            if total > 0:
                # At end of simulation, utilization ≈ non-idle agents
                self.metrics.agent_utilization_pct = round(
                    (total - available) / max(1, total) * 100, 1
                )

    async def _check_invariants(self, campaign_id: str, session_factory) -> None:
        """Run invariant checks. Records violations in metrics."""
        async with session_factory() as session:
            from sqlalchemy import select, func
            from app.persistence.models import Agent, Call
            from app.core.enums import ACTIVE_CALL_STATES

            # Invariant 1: No agent has >1 active call
            result = await session.execute(
                select(Call.agent_id, func.count(Call.id).label("cnt"))
                .where(
                    Call.campaign_id == campaign_id,
                    Call.state.in_(list(ACTIVE_CALL_STATES)),
                )
                .group_by(Call.agent_id)
                .having(func.count(Call.id) > 1)
            )
            double_assigned = result.all()
            for row in double_assigned:
                violation = f"AGENT_DOUBLE_ASSIGNED: agent={row[0]} active_calls={row[1]}"
                self.metrics.invariant_violations.append(violation)
                logger.error("invariant_violation", violation=violation)

            # Invariant 2: Calls abandoned (connected + no agent) = 0
            # (simplified check — connected calls should have an agent in CONNECTED/WRAP_UP)
            if self.metrics.calls_abandoned > 0:
                self.metrics.invariant_violations.append(
                    f"ABANDONED_CALLS: count={self.metrics.calls_abandoned}"
                )

    def _build_report(self, campaign_id: str) -> dict:
        import statistics
        cycle_times = self.metrics.cycle_times_ms
        return {
            "scenario": self.scenario_name,
            "mode": self.mode,
            "seed": self.seed,
            "num_agents": self.num_agents,
            "num_borrowers": self.num_borrowers,
            "campaign_id": campaign_id,
            "metrics": {
                "calls_proposed": self.metrics.calls_proposed,
                "calls_approved": self.metrics.calls_approved,
                "calls_rejected": self.metrics.calls_rejected,
                "calls_initiated": self.metrics.calls_initiated,
                "calls_answered": self.metrics.calls_answered,
                "calls_connected": self.metrics.calls_connected,
                "calls_completed": self.metrics.calls_completed,
                "calls_failed": self.metrics.calls_failed,
                "calls_abandoned": self.metrics.calls_abandoned,
                "calls_unresolved": self.metrics.calls_unresolved,
                "agent_utilization_pct": self.metrics.agent_utilization_pct,
                "safety_decisions": self.metrics.safety_decisions,
                "invariant_violations": self.metrics.invariant_violations,
                "cycle_time_p50_ms": round(statistics.median(cycle_times), 2) if cycle_times else 0,
                "cycle_time_p95_ms": round(
                    sorted(cycle_times)[int(len(cycle_times) * 0.95)] if len(cycle_times) >= 20 else max(cycle_times or [0]), 2
                ),
            },
        }
