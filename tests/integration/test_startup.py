"""Regression tests: fresh import, async driver dependencies and lifespan wiring."""
from __future__ import annotations

import importlib
import os
import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.database import override_engines
from app.workers.runtime import DialerRuntime
from conftest import StubProvider


def test_async_sqlalchemy_dependencies_are_installed() -> None:
    """The original failure: SQLAlchemy's asyncio extension needs greenlet."""
    import greenlet  # noqa: F401
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine("sqlite+aiosqlite://")
    assert engine.dialect.name == "sqlite"


def test_app_module_imports_and_registers_routes() -> None:
    main = importlib.import_module("app.main")
    paths = set(main.app.openapi()["paths"])
    assert "/api/v1/campaigns/{campaign_id}/dial" in paths
    assert "/api/v1/provider-events" in paths
    assert "/health" in paths


@pytest.mark.asyncio
async def test_lifespan_starts_and_stops_background_runtime(db_session: AsyncSession) -> None:
    from app.main import app

    runtime = DialerRuntime({"mock_provider_a": StubProvider()}, dispatch_interval=0.05, maintenance_interval=0.1)
    app.state.runtime = runtime
    async with app.router.lifespan_context(app):
        assert runtime.running
        task_names = {t.get_name() for t in runtime._tasks}
        assert {"dispatch-loop", "events-mock_provider_a", "maintenance-loop"} <= task_names
    assert not runtime.running
    assert all(t.done() for t in runtime._tasks) or runtime._tasks == []
    assert app.state.runtime is None


@pytest.mark.asyncio
async def test_lifespan_does_not_start_workers_without_schema(tmp_path) -> None:
    """With an empty database the app still starts (health reports the problem)
    but the background workers are not started against a missing schema."""
    from app.main import app

    db_file = tmp_path / f"empty_{uuid.uuid4().hex[:6]}.db"
    override_engines(f"sqlite:///{db_file}", f"sqlite+aiosqlite:///{db_file}")
    runtime = DialerRuntime({"mock_provider_a": StubProvider()})
    app.state.runtime = runtime
    async with app.router.lifespan_context(app):
        assert not runtime.running
    from app.persistence.database import async_engine
    await async_engine.dispose()
    if os.path.exists(db_file):
        os.remove(db_file)
