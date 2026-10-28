"""Health and readiness endpoints."""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app.persistence.database import check_database_health

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    database: str
    version: str = "0.1.0"


@router.get("/health", response_model=HealthResponse)
async def health():
    db_ok = await check_database_health()
    return HealthResponse(
        status="ok" if db_ok else "degraded",
        database="ok" if db_ok else "unavailable",
    )


@router.get("/ready")
async def ready():
    db_ok = await check_database_health()
    if not db_ok:
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail="Database unavailable")
    return {"ready": True}
