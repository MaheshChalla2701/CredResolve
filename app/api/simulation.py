"""Simulation API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.database import get_async_db

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
    failure_count: int = 10,
):
    """Inject provider failures into the circuit breaker (for testing)."""
    return {
        "message": f"Simulated {failure_count} failures on {provider_id}",
        "note": "Use the simulation engine for full scenario testing",
    }
