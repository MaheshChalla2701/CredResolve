"""Load test script — tests allocation throughput and concurrency under heavy contention.

Spawns N concurrent workers all competing to allocate calls for the same campaign.
Verifies:
  1. No duplicate call allocations (idempotency key and unique constraints).
  2. No agent assigned to two calls at once.
  3. No borrower assigned twice concurrently.
  4. Allocation throughput (ops/sec) and latency percentiles (P50, P95, P99).
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

from app.core.config import get_settings
from app.core.enums import AgentState, BorrowerStatus
from app.core.errors import AllocationError, NoAvailableAgentError, NoEligibleBorrowerError
from app.core.logging import configure_logging, get_logger
from app.persistence.database import AsyncSessionLocal, override_engines, sync_engine
from app.persistence.models import Agent, Base, Borrower, Call, Campaign
from app.services.call_allocator import CallAllocator

logger = get_logger(__name__)


async def setup_test_campaign(
    num_agents: int, num_borrowers: int
) -> tuple[str, list[str]]:
    """Seed DB with campaign, agents, and borrowers."""
    campaign_id = str(uuid.uuid4())
    async with AsyncSessionLocal() as session:
        campaign = Campaign(
            id=campaign_id,
            name=f"LOAD-TEST-{campaign_id[:8]}",
            status="ACTIVE",
            dialing_mode="progressive",
            max_concurrent_calls=num_agents * 2,
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
                max_attempts=3,
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
        except (NoAvailableAgentError, NoEligibleBorrowerError):
            stats["exhausted"] += 1
            await asyncio.sleep(0.01)  # brief backoff when pool is dry
        except AllocationError:
            stats["contention_retry"] += 1
        except Exception as exc:
            stats["errors"] += 1
            logger.debug("worker_error", worker_id=worker_id, error=str(exc))
            await asyncio.sleep(0.01)


async def verify_invariants(campaign_id: str) -> list[str]:
    """Verify that no invariants were broken during concurrent allocations."""
    violations = []
    async with AsyncSessionLocal() as session:
        from sqlalchemy import func, select

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
) -> int:
    print(f"\n=======================================================")
    print(f"  SmartDialer Concurrency & Load Test Harness")
    print(f"  Workers: {workers} | Agents: {agents} | Borrowers: {borrowers} | Duration: {duration_seconds}s")
    print(f"=======================================================\n")

    print("-> Seeding campaign data...", end="", flush=True)
    campaign_id, _ = await setup_test_campaign(agents, borrowers)
    print(" [DONE]")

    allocator = CallAllocator(provider_id="mock_provider_a")
    latencies: list[float] = []
    stats = {"success": 0, "exhausted": 0, "contention_retry": 0, "errors": 0}
    stop_event = asyncio.Event()

    print(f"-> Starting {workers} concurrent workers for {duration_seconds}s...")
    t_start = time.perf_counter()
    tasks = [
        asyncio.create_task(
            worker_task(i, campaign_id, stop_event, latencies, allocator, stats)
        )
        for i in range(workers)
    ]

    await asyncio.sleep(duration_seconds)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    t_elapsed = time.perf_counter() - t_start

    print("-> Verifying invariants post-run...", end="", flush=True)
    violations = await verify_invariants(campaign_id)
    print(" [DONE]")

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

    table_data = [
        ["Total Duration", f"{t_elapsed:.2f} s"],
        ["Successful Allocations", str(success_count)],
        ["Throughput", f"{throughput:.2f} alloc/sec"],
        ["Capacity Exhausted Hits", str(stats["exhausted"])],
        ["Contention Retries", str(stats["contention_retry"])],
        ["Unexpected Errors", str(stats["errors"])],
        ["Latency Avg", f"{avg:.2f} ms"],
        ["Latency P50", f"{p50:.2f} ms"],
        ["Latency P90", f"{p90:.2f} ms"],
        ["Latency P95", f"{p95:.2f} ms"],
        ["Latency P99", f"{p99:.2f} ms"],
        ["Invariant Violations", str(len(violations))],
    ]

    print("\n" + tabulate(table_data, headers=["Metric", "Value"], tablefmt="grid"))

    if violations:
        print("\n[CRITICAL FAILURE] Invariant violations detected:", file=sys.stderr)
        for v in violations:
            print(f"  - {v}", file=sys.stderr)
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

    args = parser.parse_args()
    configure_logging("WARNING")

    db_file = os.path.abspath(args.db_path)
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except OSError:
            pass

    sync_url = f"sqlite:///{db_file}"
    async_url = f"sqlite+aiosqlite:///{db_file}"
    new_sync_engine, _ = override_engines(sync_url, async_url)
    Base.metadata.create_all(bind=new_sync_engine)

    try:
        code = asyncio.run(
            run_load_test(
                workers=args.workers,
                agents=args.agents,
                borrowers=args.borrowers,
                duration_seconds=args.duration,
            )
        )
    finally:
        if os.path.exists(db_file):
            try:
                os.remove(db_file)
            except OSError:
                pass

    sys.exit(code)


if __name__ == "__main__":
    main()
