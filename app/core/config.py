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
    reconciliation_interval_seconds: float = 15
    dispatch_lease_seconds: int = 30
    # Active calls with no state change for this long are probed at the provider
    stale_call_seconds: int = 300

    # Background runtime (dispatch worker, provider event consumers, reconciliation)
    # started inside the API process. Disable to run the API without workers.
    background_workers_enabled: bool = True
    dispatch_poll_interval_seconds: float = 0.5
    event_max_retries: int = 3
    # Events whose provider_call_id is not yet recorded are retried until this age
    unmatched_event_max_age_seconds: int = 120

    # Provider webhook authentication (app/api/webhook_auth.py). JSON object
    # {"provider_id": "shared secret"}. Empty: webhooks are accepted without
    # authentication in development/test and refused in production.
    provider_webhook_secrets: dict[str, str] = Field(default_factory=dict)
    provider_webhook_max_skew_seconds: int = 300

    # Mock providers (the only providers wired in — no real telephony)
    mock_provider_seed: int | None = None
    # Multiplier on simulated provider delays (1.0 = realistic, 0.01 = fast tests)
    mock_provider_delay_scale: float = 1.0

    @field_validator("safety_max_dial_ratio")
    @classmethod
    def ratio_must_be_positive(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("safety_max_dial_ratio must be >= 1.0")
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
