"""Application configuration via Pydantic Settings."""
from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # App
    app_env: Literal["development", "test", "production"] = "development"
    log_level: str = "INFO"
    secret_key: str = "dev-secret-change-me"

    # Database
    database_url: str = (
        "postgresql+psycopg://smartdialer:smartdialer_dev@localhost:5432/smartdialer"
    )
    database_url_async: str = (
        "postgresql+asyncpg://smartdialer:smartdialer_dev@localhost:5432/smartdialer"
    )
    test_database_url: str = (
        "postgresql+psycopg://smartdialer_test:smartdialer_test@localhost:5433/smartdialer_test"
    )
    test_database_url_async: str = (
        "postgresql+asyncpg://smartdialer_test:smartdialer_test@localhost:5433/smartdialer_test"
    )
    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_pool_timeout: int = 30

    # Dialer
    default_dialing_mode: Literal["progressive", "predictive"] = "progressive"
    max_concurrent_calls_per_campaign: int = 50
    max_attempts_per_borrower: int = 3
    borrower_retry_cooldown_seconds: int = 3600
    agent_heartbeat_timeout_seconds: int = 30
    worker_lease_seconds: int = 60

    # Safety
    safety_max_dial_ratio: float = 3.0  # max active calls per available agent (predictive)
    safety_strict_mode: bool = True  # require 1:1 agent reservation per attempt

    # Predictive pacing
    pacer_utilization_target: float = 0.85
    pacer_smoothing_alpha: float = 0.2  # EWMA alpha for answer-rate estimation
    pacer_lookahead_seconds: int = 60
    pacer_min_samples: int = 5  # minimum samples before trusting estimate

    # Circuit breaker
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_window_seconds: int = 60
    circuit_breaker_cooldown_seconds: int = 30
    circuit_breaker_half_open_probes: int = 2

    # Reconciliation
    reconciliation_interval_seconds: int = 15
    dispatch_lease_seconds: int = 30

    @field_validator("safety_max_dial_ratio")
    @classmethod
    def ratio_must_be_positive(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("safety_max_dial_ratio must be >= 1.0")
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
