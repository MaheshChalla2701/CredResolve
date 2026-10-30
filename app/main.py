"""FastAPI application entry point."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from app.api.webhook_auth import webhook_auth_mode
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.core.metrics import get_metrics_output
from app.persistence.database import check_schema_ready, describe_database_url
from app.workers.runtime import DialerRuntime

configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info(
        "smartdialer_starting",
        env=settings.app_env,
        database=describe_database_url(settings.database_url_async),
    )
    webhook_auth = webhook_auth_mode(settings)
    if webhook_auth == "disabled":
        logger.warning(
            "provider_webhook_unauthenticated",
            hint="Set PROVIDER_WEBHOOK_SECRETS to require signed provider webhooks",
        )
    elif webhook_auth == "refused":
        logger.error(
            "provider_webhook_secrets_missing",
            hint="APP_ENV=production without PROVIDER_WEBHOOK_SECRETS: provider webhooks are refused",
        )
    runtime = getattr(app.state, "runtime", None) or DialerRuntime.with_mock_providers()
    app.state.runtime = runtime

    problem = await check_schema_ready()
    if problem is not None:
        logger.error(
            "database_not_ready",
            problem=problem,
            hint="Start the database and run `python scripts/init_db.py` (SQLite) "
                 "or `alembic upgrade head` (PostgreSQL). Background workers not started.",
        )
    elif settings.background_workers_enabled:
        await runtime.start()
    else:
        logger.warning("background_workers_disabled")

    try:
        yield
    finally:
        await runtime.stop()
        app.state.runtime = None
        logger.info("smartdialer_stopping")


app = FastAPI(
    title="SmartDialer API",
    description="Progressive + Predictive outbound dialing prototype",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Register routers ─────────────────────────────────────────────────────────
from app.api import campaigns, agents, calls, simulation, health as health_router  # noqa: E402

app.include_router(health_router.router, tags=["Health"])
app.include_router(campaigns.router, prefix="/api/v1", tags=["Campaigns"])
app.include_router(agents.router, prefix="/api/v1", tags=["Agents"])
app.include_router(calls.router, prefix="/api/v1", tags=["Calls"])
app.include_router(simulation.router, prefix="/api/v1", tags=["Simulation"])


@app.get("/metrics", include_in_schema=False)
async def metrics():
    data, content_type = get_metrics_output()
    return Response(content=data, media_type=content_type)
