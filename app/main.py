"""FastAPI application entry point."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.core.metrics import get_metrics_output
from app.persistence.database import check_database_health

configure_logging()
logger = get_logger(__name__)
_settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("smartdialer_starting", env=_settings.app_env)
    yield
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
