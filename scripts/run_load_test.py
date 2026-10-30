"""Load test script — tests allocation throughput and concurrency under heavy contention.

Modes:
  allocator (default)  N workers call CallAllocator.allocate_one() directly.
  dial                 N workers run full dialing cycles (Safety Controller +
                       allocator, as POST /dial does) while a finisher ends
                       calls, so the campaign's max_concurrent_calls is under
                       constant pressure.

Verifies:
  1. No duplicate call allocations (idempotency key and unique constraints).
  2. No agent assigned to two calls at once.
  3. No borrower assigned twice concurrently.
  4. Active calls never exceed the campaign's max_concurrent_calls (sampled
     during the run and checked at the end).
  5. dial mode: no safety decision with more calls allocated than approved.
  6. Allocation throughput (ops/sec) and latency percentiles (P50, P95, P99).

Backend: by default a temporary SQLite file. SQLite has one writer at a time and
ignores FOR UPDATE / SKIP LOCKED, so its numbers show correctness of the
compare-and-set guards and the campaign lock, NOT PostgreSQL row-lock
concurrency or throughput. For that, pass --database-url with a PostgreSQL
database already migrated with `alembic upgrade head` (rows are added, never
deleted).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
import uuid
from typing import Any

from tabulate import tabulate


def _percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])

from app.core.enums import ACTIVE_CALL_STATES, AgentState, BorrowerStatus, CallState
from app.core.errors import (
    AllocationError,
    CampaignConcurrencyLimitError,
    NoAvailableAgentError,
    NoEligibleBorrowerError,
)
from app.core.logging import configure_logging, get_logger
from app.persistence.database import AsyncSessionLocal, override_engines
from app.persistence.models import Agent, Base, Borrower, Call, Campaign, SafetyDecisionRecord
from app.services.call_allocator import CallAllocator

logger = get_logger(__name__)


async def setup_test_campaign(
    num_agents: int, num_borrowers: int, max_concurrent_calls: int | None = None
) -> tuple[str, list[str]]:
    """Seed DB with campaign, agents, and borrowers."""
    campaign_id = str(uuid.uuid4())
    async with AsyncSessionLocal() as session:
        campaign = Campaign(
            id=campaign_id,
            name=f"LOAD-TEST-{campaign_id[:8]}",
            status="ACTIVE",
            dialing_mode="progressive",
            max_concurrent_calls=max_concurrent_calls or num_agents * 2,
            retry_cooldown_seconds=0,
            max_attempts_per_borrower=1000,
        )
        session.add(campaign)

        agent_ids = []
        for i in range(num_agents):
            aid = str(uuid.uuid4())
            agent_ids.append(aid)
            agent = Agent(
                id=aid,
                campaign_id=campaign_id,
                name=f"LoadAgent-{i:04d}",
                state=AgentState.AVAILABLE,
            )
            session.add(agent)

        for j in range(num_borrowers):
            bid = str(uuid.uuid4())
            borrower = Borrower(
                id=bid,
                campaign_id=campaign_id,
                synthetic_name=f"Borrower-{j:05d}",
                masked_phone=f"+155500{j:04d}",
                status=BorrowerStatus.QUEUED,
                attempt_count=0,
                max_attempts=1000,
            )
            session.add(borrower)

        await session.commit()

    return campaign_id, agent_ids


async def worker_task(
    worker_id: int,
    campaign_id: str,
    stop_event: asyncio.Event,
    latencies: list[float],
    allocator: CallAllocator,
    stats: dict[str, int],
) -> None:
    """Worker loop attempting allocations as fast as possible until stop_event is set."""
    while not stop_event.is_set():
        t0 = time.perf_counter()
        try:
            async with AsyncSessionLocal() as session:
                call = await allocator.allocate_one(session, campaign_id)
                await session.commit()
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                latencies.append(elapsed_ms)
                stats["success"] += 1
        except (NoAvailableAgentError, NoEligibleBorrowerError, CampaignConcurrencyLimitError):
            stats["exhausted"] += 1
            await asyncio.sleep(0.01)  # brief backoff when pool is dry
        except AllocationError:
            stats["contention_retry"] += 1
        except Exception as exc:
            stats["errors"] += 1
            logger.warning("worker_error", worker_id=worker_id, error=str(exc))
            await asyncio.sleep(0.01)


async def dial_worker_task(
    worker_id: int,
    campaign_id: str,
    stop_event: asyncio.Event,
    latencies: list[float],
    stats: dict[str, int],
) -> None:
    """Worker loop running full dialing cycles (Safety Controller + allocator)."""
    from app.services.dialing import run_dialing_cycle
    from app.services.safety_controller import SafetyController

    safety = SafetyController()
    while not stop_event.is_set():
        t0 = time.perf_counter()
        try:
            async with AsyncSessionLocal() as session:
                campaign = await session.get(Campaign, campaign_id)
                result = await run_dialing_cycle(
                    session, campaign, provider_id="mock_provider_a", safety=safety
                )
                await session.commit()
            latencies.append((time.perf_counter() - t0) * 1000.0)
            stats["success"] += len(result.calls)
            stats["cycles"] += 1
            if not result.calls:
                stats["exhausted"] += 1
                await asyncio.sleep(0.01)
        except Exception as exc:
            stats["errors"] += 1
            logger.warning("dial_worker_error", worker_id=worker_id, error=str(exc))
            await asyncio.sleep(0.01)


async def end_reserved_calls(campaign_id: str, *, to_state: str, limit: int | None = None) -> int:
    """End RESERVED (never dispatched) calls the way reconciliation does: call,
    agent and borrower via finish_call(), and the PENDING outbox record marked
    FAILED so no dispatch worker sends it later."""
    from sqlalchemy import select
    from app.core.enums import DispatchStatus
    from app.persistence.repositories import CallRepository, DispatchRepository
    from app.services.call_outcomes import finish_call

    ended = 0
    async with AsyncSessionLocal() as session:
        query = select(Call.id).where(Call.campaign_id == campaign_id, Call.state == CallState.RESERVED)
        ids = (await session.execute(query.limit(limit) if limit else query)).scalars().all()
        dispatch_repo = DispatchRepository(session)
        for call_id in ids:
            call = await CallRepository(session).get_for_update(call_id)
            if call is None or not await finish_call(
                session, call, to_state=to_state, reason="LOAD_TEST", contacted=False
            ):
                continue
            dispatch = await dispatch_repo.get_by_call_id(call_id)
            if dispatch is not None and dispatch.status == DispatchStatus.PENDING:
                await dispatch_repo.update_result(dispatch.id, status=DispatchStatus.FAILED, result_code="LOAD_TEST")
            ended += 1
        await session.commit()
    return ended


async def finisher_task(campaign_id: str, stop_event: asyncio.Event, stats: dict[str, int]) -> None:
    """Ends reserved calls (as a failed attempt) so capacity keeps freeing up."""
    while not stop_event.is_set():
        try:
            stats["finished"] += await end_reserved_calls(campaign_id, to_state=CallState.FAILED, limit=5)
        except Exception as exc:
            stats["errors"] += 1
            logger.warning("finisher_error", error=str(exc))
        await asyncio.sleep(0.005)


async def cap_monitor_task(campaign_id: str, stop_event: asyncio.Event, observed: dict[str, int]) -> None:
    """Samples the number of active calls; records the maximum seen."""
    from sqlalchemy import func, select

    while not stop_event.is_set():
        async with AsyncSessionLocal() as session:
            active = (await session.execute(
                select(func.count(Call.id)).where(
                    Call.campaign_id == campaign_id, Call.state.in_(list(ACTIVE_CALL_STATES))
                )
            )).scalar_one()
        observed["max_active"] = max(observed["max_active"], active)
        observed["samples"] += 1
        await asyncio.sleep(0.002)


async def verify_invariants(campaign_id: str, max_active_observed: int) -> list[str]:
    """Verify that no invariants were broken during concurrent allocations."""
    violations = []
    async with AsyncSessionLocal() as session:
        from sqlalchemy import func, select

        campaign = await session.get(Campaign, campaign_id)
        cap = campaign.max_concurrent_calls
        active_now = (await session.execute(
            select(func.count(Call.id)).where(
                Call.campaign_id == campaign_id, Call.state.in_(list(ACTIVE_CALL_STATES))
            )
        )).scalar_one()
        if max(active_now, max_active_observed) > cap:
            violations.append(
                f"Active calls exceeded max_concurrent_calls={cap}: "
                f"max sampled {max_active_observed}, at end {active_now}"
            )

        decisions = (await session.execute(
            select(SafetyDecisionRecord).where(SafetyDecisionRecord.campaign_id == campaign_id)
        )).scalars().all()
        over = [d.id for d in decisions
                if (d.capacity_snapshot or {}).get("allocated_count", 0) > d.approved_count]
        if over:
            violations.append(f"{len(over)} safety decisions allocated more than approved: {over[:5]}")

        # 1. Check for duplicate idempotency keys
        dup_keys = await session.execute(
            select(Call.idempotency_key, func.count(Call.id))
            .where(Call.campaign_id == campaign_id)
            .group_by(Call.idempotency_key)
            .having(func.count(Call.id) > 1)
        )
        dups = dup_keys.all()
        if dups:
            violations.append(f"Found {len(dups)} duplicate call idempotency keys: {dups}")

        # 2. Check no agent has multiple concurrent active calls
        agent_active = await session.execute(
            select(Call.agent_id, func.count(Call.id))
            .where(
                Call.campaign_id == campaign_id,
                Call.state.in_(["RESERVED", "INITIATED", "RINGING", "ANSWERED", "CONNECTED"]),
            )
            .group_by(Call.agent_id)
            .having(func.count(Call.id) > 1)
        )
        active_dups = agent_active.all()
        if active_dups:
            violations.append(f"Found {len(active_dups)} agents with >1 active call: {active_dups}")

        # 3. Check no borrower has multiple concurrent active calls
        borrower_active = await session.execute(
            select(Call.borrower_id, func.count(Call.id))
            .where(
                Call.campaign_id == campaign_id,
                Call.state.in_(["RESERVED", "INITIATED", "RINGING", "ANSWERED", "CONNECTED"]),
            )
            .group_by(Call.borrower_id)
            .having(func.count(Call.id) > 1)
        )
        b_dups = borrower_active.all()
        if b_dups:
            violations.append(f"Found {len(b_dups)} borrowers with >1 active call: {b_dups}")

    return violations


async def run_load_test(
    workers: int,
    agents: int,
    borrowers: int,
    duration_seconds: int,
    *,
    mode: str = "allocator",
    max_concurrent_calls: int | None = None,
    backend: str = "SQLite",
) -> int:
    print("\n=======================================================")
    print("  SmartDialer Concurrency & Load Test Harness")
    print(f"  Mode: {mode} | Backend: {backend}")
    print(f"  Workers: {workers} | Agents: {agents} | Borrowers: {borrowers} | Duration: {duration_seconds}s")
    print("=======================================================\n")

    print("-> Seeding campaign data...", end="", flush=True)
    campaign_id, _ = await setup_test_campaign(agents, borrowers, max_concurrent_calls)
    print(" [DONE]")

    allocator = CallAllocator(provider_id="mock_provider_a")
    latencies: list[float] = []
    stats = {"success": 0, "exhausted": 0, "contention_retry": 0, "errors": 0, "cycles": 0, "finished": 0}
    observed = {"max_active": 0, "samples": 0}
    stop_event = asyncio.Event()

    print(f"-> Starting {workers} concurrent workers for {duration_seconds}s...")
    t_start = time.perf_counter()
    if mode == "dial":
        tasks = [
            asyncio.create_task(dial_worker_task(i, campaign_id, stop_event, latencies, stats))
            for i in range(workers)
        ]
        tasks.append(asyncio.create_task(finisher_task(campaign_id, stop_event, stats)))
    else:
        tasks = [
            asyncio.create_task(
                worker_task(i, campaign_id, stop_event, latencies, allocator, stats)
            )
            for i in range(workers)
        ]
    tasks.append(asyncio.create_task(cap_monitor_task(campaign_id, stop_event, observed)))

    await asyncio.sleep(duration_seconds)
    stop_event.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    t_elapsed = time.perf_counter() - t_start
    for result in results:
        if isinstance(result, Exception):
            stats["errors"] += 1
            print(f"  task failed: {result!r}", file=sys.stderr)

    print("-> Verifying invariants post-run...", end="", flush=True)
    violations = await verify_invariants(campaign_id, observed["max_active"])
    print(" [DONE]")

    # Leave nothing dispatchable behind (matters when --database-url is shared
    # with an API whose dispatch worker would otherwise send these calls).
    from app.persistence.repositories import CampaignRepository
    cancelled = await end_reserved_calls(campaign_id, to_state=CallState.CANCELLED)
    async with AsyncSessionLocal() as session:
        await CampaignRepository(session).update_status(campaign_id, "PAUSED")
        await session.commit()
    print(f"-> Cancelled {cancelled} leftover reserved calls; campaign paused.")

    # Compute percentiles
    success_count = stats["success"]
    throughput = success_count / t_elapsed if t_elapsed > 0 else 0

    if latencies:
        p50 = _percentile(latencies, 50)
        p90 = _percentile(latencies, 90)
        p95 = _percentile(latencies, 95)
        p99 = _percentile(latencies, 99)
        avg = sum(latencies) / len(latencies)
    else:
        p50 = p90 = p95 = p99 = avg = 0.0

    async with AsyncSessionLocal() as session:
        cap = (await session.get(Campaign, campaign_id)).max_concurrent_calls

    table_data = [
        ["Backend", backend],
        ["Mode", mode],
        ["Total Duration", f"{t_elapsed:.2f} s"],
        ["Successful Allocations", str(success_count)],
        ["Throughput", f"{throughput:.2f} alloc/sec"],
        ["Capacity Exhausted Hits", str(stats["exhausted"])],
        ["Contention Retries", str(stats["contention_retry"])],
        ["Unexpected Errors", str(stats["errors"])],
        ["Dialing Cycles (dial mode)", str(stats["cycles"])],
        ["Calls Finished (dial mode)", str(stats["finished"])],
        ["Max Active Calls Sampled / Cap", f"{observed['max_active']} / {cap} ({observed['samples']} samples)"],
        ["Latency Avg", f"{avg:.2f} ms"],
        ["Latency P50", f"{p50:.2f} ms"],
        ["Latency P90", f"{p90:.2f} ms"],
        ["Latency P95", f"{p95:.2f} ms"],
        ["Latency P99", f"{p99:.2f} ms"],
        ["Invariant Violations", str(len(violations))],
    ]

    print("\n" + tabulate(table_data, headers=["Metric", "Value"], tablefmt="grid"))

    if backend == "SQLite":
        print("\nNote: SQLite serializes writers and ignores FOR UPDATE / SKIP LOCKED. These "
              "results validate the guards, not PostgreSQL concurrency or throughput.")

    if violations:
        print("\n[CRITICAL FAILURE] Invariant violations detected:", file=sys.stderr)
        for v in violations:
            print(f"  - {v}", file=sys.stderr)
        return 1
    if stats["errors"]:
        print(f"\n[FAILURE] {stats['errors']} unexpected errors (see warnings above).", file=sys.stderr)
        return 1

    print("\n[SUCCESS] Load test passed with ZERO invariant violations!\n")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="SmartDialer Allocation Load Test")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent worker tasks")
    parser.add_argument("--agents", type=int, default=100, help="Number of agents in campaign")
    parser.add_argument("--borrowers", type=int, default=500, help="Number of borrowers in campaign")
    parser.add_argument("--duration", type=int, default=15, help="Test duration in seconds")
    parser.add_argument("--db-path", default="load_test.db", help="SQLite database path for standalone runs")
    parser.add_argument("--mode", choices=["allocator", "dial"], default="allocator",
                        help="allocator: direct allocate_one(); dial: full Safety Controller dialing cycles")
    parser.add_argument("--max-concurrent", type=int, default=None,
                        help="Campaign max_concurrent_calls (default: 2 × agents; set below --agents to stress the cap)")
    parser.add_argument("--database-url", default=None,
                        help="Async PostgreSQL URL (postgresql+asyncpg://...) of a database already at "
                             "`alembic upgrade head`. Rows are added, never deleted. Default: temporary SQLite file.")

    args = parser.parse_args()
    configure_logging("WARNING")

    async def run_and_close(backend: str) -> int:
        try:
            return await run_load_test(
                workers=args.workers,
                agents=args.agents,
                borrowers=args.borrowers,
                duration_seconds=args.duration,
                mode=args.mode,
                max_concurrent_calls=args.max_concurrent,
                backend=backend,
            )
        finally:
            from app.persistence import database

            await database.async_engine.dispose()
            database.sync_engine.dispose()

    run = lambda backend: asyncio.run(run_and_close(backend))  # noqa: E731

    if args.database_url:
        if not args.database_url.startswith("postgresql+asyncpg://"):
            parser.error("--database-url must be a postgresql+asyncpg:// URL")
        override_engines(args.database_url.replace("+asyncpg", "+psycopg"), args.database_url)
        sys.exit(run("PostgreSQL"))

    db_file = os.path.abspath(args.db_path)
    if os.path.exists(db_file):
        os.remove(db_file)

    sync_url = f"sqlite:///{db_file}"
    async_url = f"sqlite+aiosqlite:///{db_file}"
    new_sync_engine, _ = override_engines(sync_url, async_url)
    Base.metadata.create_all(bind=new_sync_engine)

    try:
        code = run("SQLite")
    finally:
        if os.path.exists(db_file):
            os.remove(db_file)

    sys.exit(code)


if __name__ == "__main__":
    main()
