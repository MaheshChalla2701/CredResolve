"""Simulation API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.database import get_async_db
from app.workers.runtime import DialerRuntime, get_runtime

router = APIRouter()


async def get_db():
    async with get_async_db() as session:
        yield session


class SimulationRequest(BaseModel):
    scenario: str = Field(
        default="medium_answer_rate",
        description="Scenario name: low_answer_rate, medium_answer_rate, high_answer_rate, variable_answer_rate, provider_degradation, agent_churn, burst_answering"
    )
    mode: str = Field(default="progressive", pattern="^(progressive|predictive)$")
    seed: int = Field(default=42)
    num_agents: int = Field(default=20, ge=1, le=500)
    num_borrowers: int = Field(default=100, ge=1, le=5000)


@router.post("/simulation/run")
async def run_simulation(
    body: SimulationRequest,
    session: AsyncSession = Depends(get_db),
):
    """Run a simulation scenario and return metrics."""
    from app.simulation.engine import SimulationEngine

    engine = SimulationEngine(
        scenario_name=body.scenario,
        mode=body.mode,
        seed=body.seed,
        num_agents=body.num_agents,
        num_borrowers=body.num_borrowers,
        db_session=session,
    )
    results = await engine.run()
    return results


@router.post("/simulation/provider-failure")
async def simulate_provider_failure(
    provider_id: str = "mock_provider_b",
    failure_count: int = Query(default=10, ge=1, le=1000),
    session: AsyncSession = Depends(get_db),
    runtime: DialerRuntime = Depends(get_runtime),
):
    """Inject provider failures into the circuit breaker (for testing).

    Records `failure_count` failures exactly as the dispatch worker does after a
    failed provider request. At circuit_breaker_failure_threshold failures the
    circuit opens: the Safety Controller then rejects dialing on this provider
    and the predictive pacer proposes nothing, until the cooldown elapses and
    half-open probe calls succeed. The mock provider itself is not changed.
    """
    health = runtime.health.get(provider_id)
    if health is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown provider_id {provider_id!r}; available: {sorted(runtime.health)}",
        )
    for _ in range(failure_count):
        await health.record_failure(session)
    snapshot = await health.get_snapshot(session)
    return {
        "provider_id": provider_id,
        "failures_injected": failure_count,
        "circuit_state": snapshot.get("circuit_state"),
        "provider_health": snapshot,
    }
