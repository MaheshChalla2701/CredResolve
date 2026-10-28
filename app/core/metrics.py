"""Prometheus-compatible metrics using prometheus_client."""
from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# Use a custom registry so tests can reset state
REGISTRY = CollectorRegistry(auto_describe=True)

# ── Call lifecycle counters ──────────────────────────────────────────────────
calls_proposed = Counter(
    "smartdialer_calls_proposed_total",
    "Total calls proposed by dialing engine",
    ["campaign_id", "mode"],
    registry=REGISTRY,
)
calls_approved = Counter(
    "smartdialer_calls_approved_total",
    "Total calls approved by safety controller",
    ["campaign_id"],
    registry=REGISTRY,
)
calls_rejected = Counter(
    "smartdialer_calls_rejected_total",
    "Total calls rejected by safety controller",
    ["campaign_id", "reason"],
    registry=REGISTRY,
)
calls_initiated = Counter(
    "smartdialer_calls_initiated_total",
    "Total calls dispatched to provider",
    ["campaign_id", "provider"],
    registry=REGISTRY,
)
calls_answered = Counter(
    "smartdialer_calls_answered_total",
    "Total calls answered",
    ["campaign_id"],
    registry=REGISTRY,
)
calls_connected = Counter(
    "smartdialer_calls_connected_total",
    "Total calls connected to agent",
    ["campaign_id"],
    registry=REGISTRY,
)
calls_completed = Counter(
    "smartdialer_calls_completed_total",
    "Total calls completed successfully",
    ["campaign_id"],
    registry=REGISTRY,
)
calls_failed = Counter(
    "smartdialer_calls_failed_total",
    "Total calls failed",
    ["campaign_id", "reason"],
    registry=REGISTRY,
)
calls_abandoned = Counter(
    "smartdialer_calls_abandoned_total",
    "Total answered calls abandoned (no agent available) — MUST REMAIN ZERO",
    ["campaign_id"],
    registry=REGISTRY,
)

# ── Agent metrics ────────────────────────────────────────────────────────────
agents_available = Gauge(
    "smartdialer_agents_available",
    "Current available agents",
    ["campaign_id"],
    registry=REGISTRY,
)
agents_reserved = Gauge(
    "smartdialer_agents_reserved",
    "Current reserved/dialing/connected agents",
    ["campaign_id"],
    registry=REGISTRY,
)

# ── Event processing ─────────────────────────────────────────────────────────
events_received = Counter(
    "smartdialer_provider_events_received_total",
    "Total provider events received",
    ["provider", "event_type"],
    registry=REGISTRY,
)
events_deduplicated = Counter(
    "smartdialer_provider_events_deduplicated_total",
    "Total duplicate provider events suppressed",
    ["provider"],
    registry=REGISTRY,
)
events_out_of_order = Counter(
    "smartdialer_provider_events_out_of_order_total",
    "Events that arrived out of order",
    ["provider"],
    registry=REGISTRY,
)

# ── Safety decisions ─────────────────────────────────────────────────────────
safety_decisions = Counter(
    "smartdialer_safety_decisions_total",
    "Safety controller decisions",
    ["campaign_id", "decision"],
    registry=REGISTRY,
)

# ── Provider health ──────────────────────────────────────────────────────────
provider_requests = Counter(
    "smartdialer_provider_requests_total",
    "Outbound provider requests",
    ["provider", "outcome"],
    registry=REGISTRY,
)
circuit_breaker_state = Gauge(
    "smartdialer_circuit_breaker_state",
    "Circuit breaker state: 0=CLOSED, 1=OPEN, 2=HALF_OPEN",
    ["provider"],
    registry=REGISTRY,
)

# ── Latency ──────────────────────────────────────────────────────────────────
allocation_latency = Histogram(
    "smartdialer_allocation_latency_seconds",
    "Time to complete atomic allocation",
    ["campaign_id"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
    registry=REGISTRY,
)
event_processing_latency = Histogram(
    "smartdialer_event_processing_latency_seconds",
    "Time to process a provider event end-to-end",
    ["provider"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
    registry=REGISTRY,
)

# ── Reconciliation ───────────────────────────────────────────────────────────
reconciliation_actions = Counter(
    "smartdialer_reconciliation_actions_total",
    "Reconciliation actions taken",
    ["action_type"],
    registry=REGISTRY,
)


def get_metrics_output() -> tuple[bytes, str]:
    """Return Prometheus text format and content type."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
