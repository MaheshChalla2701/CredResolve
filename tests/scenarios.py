"""Concurrency scenarios shared by the SQLite suite and tests/postgres.

Each takes a session factory, so the same assertions run against whichever
database the caller configured.
"""
from __future__ import annotations

import asyncio
import random
import uuid
from datetime import timedelta

from sqlalchemy import select

from app.core.enums import AgentState, BorrowerStatus, CallState, DispatchStatus
from app.core.timeutils import utcnow
from app.persistence.models import Agent, Borrower, Call, Campaign, DispatchRecord, SafetyDecisionRecord
from app.persistence.repositories import AgentRepository
from app.persistence.retry import run_transaction
from app.services.dialing import run_dialing_cycle
from app.services.event_processor import EventProcessor
from app.services.reconciliation import ReconciliationService
from app.services.safety_controller import SafetyController


class GoneProvider:
    """Provider adapter stub whose calls are all unknown (outcome lost)."""

    provider_id = "scenario"

    async def get_call_status(self, provider_call_id: str):
        await asyncio.sleep(random.random() * 0.01)
        return None


async def reconciliation_vs_events(factory, *, calls: int = 30, reconcilers: int = 2, rounds: int = 3) -> dict:
    """Calls whose outcome is "lost" (reconciliation will finish them as FAILED)
    while their real outcome events ("completed" / "busy") arrive at the same
    time. Exactly one path must finish each call, and agent and borrower must
    match the call's final state. Returns counts for the caller to assert on."""
    cid = str(uuid.uuid4())
    old = utcnow() - timedelta(hours=1)
    async with factory() as s:
        s.add(Campaign(id=cid, name=f"rv-{cid[:8]}", status="ACTIVE", wrap_up_seconds=0,
                       retry_cooldown_seconds=0, max_concurrent_calls=calls))
        await s.flush()
        for i in range(calls):
            call_id, agent_id, borrower_id = (str(uuid.uuid4()) for _ in range(3))
            s.add(Agent(id=agent_id, campaign_id=cid, name=f"A{i}", state=AgentState.DIALING,
                        current_call_id=call_id))
            s.add(Borrower(id=borrower_id, campaign_id=cid, synthetic_name=f"B{i}", masked_phone=f"***-{i}",
                           status=BorrowerStatus.ALLOCATED, attempt_count=1))
            await s.flush()
            s.add(Call(id=call_id, campaign_id=cid, agent_id=agent_id, borrower_id=borrower_id,
                       provider_id="scenario", provider_call_id=f"rv-{cid[:8]}-{i}",
                       idempotency_key=f"rv-{cid}-{i}", attempt_number=1, state=CallState.INITIATED,
                       initiated_at=old, updated_at=old, created_at=old))
            await s.flush()
            s.add(DispatchRecord(call_id=call_id, campaign_id=cid, provider_id="scenario",
                                 idempotency_key=f"rv-{cid}-{i}", masked_phone="***",
                                 status=DispatchStatus.SUCCEEDED, provider_call_id=f"rv-{cid[:8]}-{i}"))
        await s.commit()

    reconciler = ReconciliationService(provider_adapters={"scenario": GoneProvider()})
    processor = EventProcessor()
    errors: list[str] = []

    async def reconcile_loop() -> None:
        for _ in range(rounds):
            async with factory() as s:
                result = await reconciler.run(s)
            errors.extend(result["errors"])
            await asyncio.sleep(random.random() * 0.01)

    async def deliver(i: int) -> None:
        await asyncio.sleep(random.random() * 0.05)
        event_type = "completed" if i % 2 == 0 else "busy"

        async def work(s):
            return await processor.process_raw(
                s, provider_id="scenario", provider_event_id=f"rv-{cid}-{i}",
                provider_call_id=f"rv-{cid[:8]}-{i}", event_type=event_type, payload={},
            )
        try:
            async with factory() as s:
                await run_transaction(s, work, op="scenario_event")
        except Exception as exc:  # recorded, asserted empty by the caller
            errors.append(f"event {i}: {type(exc).__name__}: {exc}")

    await asyncio.gather(*[reconcile_loop() for _ in range(reconcilers)], *[deliver(i) for i in range(calls)])

    inconsistent: list[str] = []
    finished_by_event = 0
    async with factory() as s:
        rows = (await s.execute(
            select(Call, Agent, Borrower)
            .join(Agent, Call.agent_id == Agent.id)
            .join(Borrower, Call.borrower_id == Borrower.id)
            .where(Call.campaign_id == cid)
        )).all()
    for call, agent, borrower in rows:
        if call.failure_reason != "RECONCILE_OUTCOME_LOST":
            finished_by_event += 1
        if agent.current_call_id is not None:
            inconsistent.append(f"{call.id}: agent still holds call ({agent.state})")
        if call.state == CallState.COMPLETED:
            ok = agent.state in (AgentState.WRAP_UP, AgentState.AVAILABLE) and borrower.status == BorrowerStatus.COMPLETED
        elif call.state == CallState.FAILED:
            ok = agent.state == AgentState.AVAILABLE and borrower.status == BorrowerStatus.QUEUED
        else:
            ok = False
        if not ok:
            inconsistent.append(f"{call.id}: call={call.state} agent={agent.state} borrower={borrower.status}")
    return {"calls": len(rows), "errors": errors, "inconsistent": inconsistent,
            "finished_by_event": finished_by_event}


class PausingSafetyController(SafetyController):
    """Runs `after_decision()` between the Safety Controller's decision and the
    allocation — the window in which an agent can change availability."""

    def __init__(self, after_decision, **kwargs) -> None:
        super().__init__(**kwargs)
        self._after_decision = after_decision

    async def evaluate(self, session, **kwargs):
        decision = await super().evaluate(session, **kwargs)
        await self._after_decision(session)
        return decision


async def availability_campaign(factory, *, agents: int, borrowers: int) -> tuple[str, list[str]]:
    cid = str(uuid.uuid4())
    async with factory() as s:
        s.add(Campaign(id=cid, name=f"av-{cid[:8]}", status="ACTIVE", max_concurrent_calls=50))
        await s.flush()
        ids = []
        for i in range(agents):
            agent = Agent(campaign_id=cid, name=f"A{i}", state=AgentState.AVAILABLE)
            s.add(agent)
            await s.flush()
            ids.append(agent.id)
        for i in range(borrowers):
            s.add(Borrower(campaign_id=cid, synthetic_name=f"B{i}", masked_phone=f"***-{i}", status="QUEUED"))
        await s.commit()
    return cid, ids


async def pause_agent(session, agent_id: str, to_state: str = AgentState.PAUSED) -> bool:
    """What POST /agents/{id}/availability does: lock, validate, conditional update."""
    from app.domain.state_machines import validate_agent_transition

    repo = AgentRepository(session)
    agent = await repo.get_for_update(agent_id)
    validate_agent_transition(agent.state, to_state)
    return await repo.transition_state(agent_id, from_state=agent.state, to_state=to_state, version=agent.version)


async def dial(factory, cid: str, safety: SafetyController):
    async with factory() as s:
        result = await run_dialing_cycle(s, await s.get(Campaign, cid), provider_id="mock_provider_a", safety=safety)
        await s.commit()
    return result


async def availability_state(factory, cid: str) -> dict:
    """Everything the agent-availability race could leave inconsistent."""
    async with factory() as s:
        agents = (await s.execute(select(Agent).where(Agent.campaign_id == cid))).scalars().all()
        borrowers = (await s.execute(select(Borrower).where(Borrower.campaign_id == cid))).scalars().all()
        calls = (await s.execute(select(Call).where(Call.campaign_id == cid))).scalars().all()
        dispatches = (await s.execute(select(DispatchRecord).where(DispatchRecord.campaign_id == cid))).scalars().all()
        [record] = (await s.execute(
            select(SafetyDecisionRecord).where(SafetyDecisionRecord.campaign_id == cid))).scalars().all()
    call_by_agent = {c.agent_id: c for c in calls}
    return {
        "agents": {a.id: (a.state, a.current_call_id) for a in agents},
        "reserved_agents_with_their_call": sum(
            1 for a in agents
            if a.state == AgentState.RESERVED and a.current_call_id == getattr(call_by_agent.get(a.id), "id", None)
        ),
        "borrowers": sorted((b.status, b.attempt_count) for b in borrowers),
        "calls": len(calls),
        "pending_dispatches": sum(1 for d in dispatches if d.status == DispatchStatus.PENDING),
        "record": (record.decision, record.approved_count, record.capacity_snapshot.get("allocated_count"),
                   list(record.reason_codes)),
    }
