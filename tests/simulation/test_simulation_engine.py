"""Tests for Simulation Engine execution and invariant preservation."""
from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.simulation.engine import SimulationEngine


@pytest.mark.asyncio
class TestSimulationEngine:
    async def test_run_progressive_simulation(self, db_session: AsyncSession) -> None:
        """Run progressive simulation and verify 0 invariant violations."""
        engine = SimulationEngine(
            scenario_name="low_answer_rate",
            mode="progressive",
            seed=42,
            num_agents=5,
            num_borrowers=20,
        )
        report = await engine.run()

        assert report["mode"] == "progressive"
        assert report["scenario"] == "low_answer_rate"
        metrics = report["metrics"]
        assert metrics["calls_initiated"] > 0
        # Critical guarantee: zero invariant violations
        assert len(metrics["invariant_violations"]) == 0
        assert metrics.get("calls_abandoned", 0) == 0

    async def test_run_predictive_simulation(self, db_session: AsyncSession) -> None:
        """Run predictive simulation and verify 0 invariant violations."""
        engine = SimulationEngine(
            scenario_name="medium_answer_rate",
            mode="predictive",
            seed=100,
            num_agents=8,
            num_borrowers=30,
        )
        report = await engine.run()

        assert report["mode"] == "predictive"
        assert report["scenario"] == "medium_answer_rate"
        metrics = report["metrics"]
        assert metrics["calls_initiated"] > 0
        assert len(metrics["invariant_violations"]) == 0
