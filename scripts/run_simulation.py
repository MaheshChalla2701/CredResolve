"""Simulation CLI runner — evaluates and compares Progressive vs Predictive dialing.

Runs the 7 configured scenarios and reports key metrics:
  - Agent utilization percentage
  - Abandonment rate percentage (target <= 3%)
  - Completed calls throughput
  - Invariant violations count (must be 0)
  - Cycle latency (p50 / p95)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import os
from typing import Any

from tabulate import tabulate

from app.core.logging import configure_logging, get_logger
from app.persistence.database import override_engines, async_engine, sync_engine
from app.persistence.models import Base
from app.simulation.engine import SCENARIO_CONFIGS, SimulationEngine

logger = get_logger(__name__)


async def init_simulation_db(sync_url: str, async_url: str) -> None:
    """Initialize DB schema for simulation."""
    new_sync_engine, _ = override_engines(sync_url, async_url)
    # Create all tables on the overridden sync engine
    Base.metadata.create_all(bind=new_sync_engine)


async def run_single_simulation(
    scenario: str,
    mode: str,
    seed: int,
    agents: int,
    borrowers: int,
    delay_scale: float = 1.0,
    drain_timeout: float = 60.0,
) -> dict[str, Any]:
    """Run a single simulation run and return metrics."""
    engine = SimulationEngine(
        scenario_name=scenario,
        mode=mode,
        seed=seed,
        num_agents=agents,
        num_borrowers=borrowers,
        delay_scale=delay_scale,
        drain_timeout_seconds=drain_timeout,
    )
    return await engine.run()


def format_table(results: list[dict[str, Any]]) -> str:
    """Format comparison results as an ASCII table."""
    headers = [
        "Scenario",
        "Mode",
        "Agents",
        "Proposed",
        "Approved",
        "Initiated",
        "Answered",
        "Connected",
        "Completed",
        "Failed",
        "Abandoned",
        "Unresolved",
        "Talk util %",
        "Aband %",
        "Safety A/R/X",
        "Violations",
        "Drained",
    ]
    rows = []
    for r in results:
        m = r.get("metrics", r)
        sd = m.get("safety_decisions", {})
        rows.append([
            r.get("scenario", "-"),
            r.get("mode", "-").upper(),
            r.get("num_agents", r.get("agents", 0)),
            m.get("calls_proposed", 0),
            m.get("calls_approved", 0),
            m.get("calls_initiated", 0),
            m.get("calls_answered", 0),
            m.get("calls_connected", 0),
            m.get("calls_completed", 0),
            m.get("calls_failed", 0),
            m.get("calls_abandoned", 0),
            m.get("calls_unresolved", 0),
            f"{m.get('agent_utilization_pct', 0.0):.1f}%",
            f"{m.get('abandonment_rate_pct', 0.0):.2f}%",
            f"{sd.get('APPROVE', 0)}/{sd.get('REDUCE', 0)}/{sd.get('REJECT', 0)}",
            len(m.get("invariant_violations", [])),
            "yes" if r.get("completed") else "TIMEOUT",
        ])
    return tabulate(rows, headers=headers, tablefmt="grid")


async def main_async() -> int:
    parser = argparse.ArgumentParser(
        description="SmartDialer Simulation Runner: Compare Progressive vs Predictive Pacing"
    )
    parser.add_argument(
        "--scenario",
        default="all",
        choices=["all"] + list(SCENARIO_CONFIGS.keys()),
        help="Scenario to run (or 'all')",
    )
    parser.add_argument(
        "--mode",
        default="both",
        choices=["both", "progressive", "predictive"],
        help="Dialing mode to simulate",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--agents", type=int, default=15, help="Number of simulated agents")
    parser.add_argument("--borrowers", type=int, default=100, help="Number of simulated borrowers")
    parser.add_argument("--format", default="table", choices=["table", "json"], help="Output format")
    parser.add_argument("--db-path", default="simulation.db", help="SQLite database path for standalone runs")
    parser.add_argument(
        "--delay-scale", type=float, default=1.0,
        help="Multiplier on simulated provider delays (e.g. 0.1 runs ~10x faster)",
    )
    parser.add_argument(
        "--drain-timeout", type=float, default=60.0,
        help="Seconds to wait for all calls to finish after the last dialing cycle",
    )

    args = parser.parse_args()
    configure_logging("WARNING")

    # Set up standalone simulation DB
    db_file = os.path.abspath(args.db_path)
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except OSError:
            pass

    sync_url = f"sqlite:///{db_file}"
    async_url = f"sqlite+aiosqlite:///{db_file}"
    await init_simulation_db(sync_url, async_url)

    scenarios = list(SCENARIO_CONFIGS.keys()) if args.scenario == "all" else [args.scenario]
    modes = ["progressive", "predictive"] if args.mode == "both" else [args.mode]

    results: list[dict[str, Any]] = []

    print(f"\n=======================================================")
    print(f"  SmartDialer Simulation Suite (Seed={args.seed})")
    print(f"  Running {len(scenarios)} scenarios x {len(modes)} modes")
    print(f"=======================================================\n")

    for sc in scenarios:
        for md in modes:
            print(f"-> Running scenario: {sc:<22} mode: {md:<12} ...", end="", flush=True)
            res = await run_single_simulation(
                scenario=sc,
                mode=md,
                seed=args.seed,
                agents=args.agents,
                borrowers=args.borrowers,
                delay_scale=args.delay_scale,
                drain_timeout=args.drain_timeout,
            )
            results.append(res)
            print(" [DONE]" if res.get("completed") else " [TIMEOUT: calls still unresolved]")

    print("\n" + "=" * 55 + " RESULTS " + "=" * 55 + "\n")
    if args.format == "json":
        print(json.dumps(results, indent=2))
    else:
        print(format_table(results))

    # Clean up DB file
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except OSError:
            pass

    # A run only counts as a success if it drained (no unresolved calls) and
    # broke no invariants.
    total_violations = sum(len(r.get("metrics", r).get("invariant_violations", [])) for r in results)
    total_unresolved = sum(r.get("metrics", r).get("calls_unresolved", 0) for r in results)
    timed_out = [f"{r['scenario']}/{r['mode']}" for r in results if not r.get("completed")]
    if total_violations or total_unresolved or timed_out:
        print(
            f"\n[FAILED] violations={total_violations} unresolved_calls={total_unresolved} "
            f"timed_out={timed_out or 'none'}",
            file=sys.stderr,
        )
        return 1

    print("\n[SUCCESS] All scenarios drained: every call reached a final state, 0 invariant violations.\n")
    return 0


async def _run_and_close() -> int:
    try:
        return await main_async()
    finally:
        # Close pooled connections before the event loop goes away (otherwise
        # aiosqlite connection threads are left open until interpreter exit).
        from app.persistence import database

        await database.async_engine.dispose()
        database.sync_engine.dispose()


def main() -> None:
    sys.exit(asyncio.run(_run_and_close()))


if __name__ == "__main__":
    main()
