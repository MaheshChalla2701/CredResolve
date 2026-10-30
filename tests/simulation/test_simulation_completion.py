"""Regression tests: the simulation waits for provider outcomes and cannot report
success while calls are unresolved."""
from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.simulation.engine import SimulationEngine


@pytest.mark.asyncio
class TestSimulationCompletion:
    @pytest.mark.parametrize("mode", ["progressive", "predictive"])
    async def test_simulation_drains_all_calls(self, db_session: AsyncSession, mode: str) -> None:
        report = await SimulationEngine(
            scenario_name="medium_answer_rate", mode=mode, seed=7,
            num_agents=4, num_borrowers=12, delay_scale=0.05, drain_timeout_seconds=30,
        ).run()
        m = report["metrics"]

        assert report["completed"] is True and report["success"] is True
        assert m["calls_unresolved"] == 0
        assert m["invariant_violations"] == []
        assert m["calls_initiated"] > 0
        # Every reserved call reached exactly one final state.
        assert m["calls_reserved"] == m["calls_completed"] + m["calls_failed"] + m["calls_cancelled"]
        assert m["calls_answered"] >= m["calls_connected"] and m["calls_answered"] >= m["calls_completed"]
        # Safety decisions are counted from the audit table in both modes.
        assert sum(m["safety_decisions"].values()) >= 1
        assert 0.0 <= m["agent_utilization_pct"] <= 100.0
        if mode == "predictive":
            assert m["pacing_decisions"] > 0

    async def test_timeout_reports_unresolved_calls_and_no_success(self, db_session: AsyncSession) -> None:
        report = await SimulationEngine(
            scenario_name="medium_answer_rate", mode="progressive", seed=7,
            num_agents=4, num_borrowers=8, delay_scale=1.0, drain_timeout_seconds=0.0,
        ).run()
        m = report["metrics"]
        assert report["completed"] is False
        assert report["success"] is False
        assert m["calls_unresolved"] > 0
