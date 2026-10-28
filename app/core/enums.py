"""Domain enumerations — the canonical set of states and types."""
from __future__ import annotations

from enum import StrEnum


class AgentState(StrEnum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class CallState(StrEnum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class CampaignStatus(StrEnum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    ARCHIVED = "ARCHIVED"


class DialingMode(StrEnum):
    PROGRESSIVE = "progressive"
    PREDICTIVE = "predictive"


class BorrowerStatus(StrEnum):
    QUEUED = "QUEUED"
    ALLOCATED = "ALLOCATED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DNC = "DNC"              # do-not-call
    EXHAUSTED = "EXHAUSTED"  # max attempts reached


class ProviderEventType(StrEnum):
    INITIATED = "initiated"
    RINGING = "ringing"
    ANSWERED = "answered"
    CONNECTED = "connected"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NO_ANSWER = "no_answer"
    BUSY = "busy"


class CircuitBreakerState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class DispatchStatus(StrEnum):
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    SENT = "SENT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    AMBIGUOUS = "AMBIGUOUS"


class SafetyDecision(StrEnum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"
    FALLBACK_TO_PROGRESSIVE = "FALLBACK_TO_PROGRESSIVE"


class AuditEventType(StrEnum):
    CAMPAIGN_CREATED = "campaign_created"
    CAMPAIGN_STARTED = "campaign_started"
    CAMPAIGN_PAUSED = "campaign_paused"
    AGENT_STATE_CHANGED = "agent_state_changed"
    CALL_STATE_CHANGED = "call_state_changed"
    BORROWER_ALLOCATED = "borrower_allocated"
    SAFETY_DECISION = "safety_decision"
    PROVIDER_EVENT_RECEIVED = "provider_event_received"
    RECONCILIATION_ACTION = "reconciliation_action"
    DISPATCH_ATTEMPTED = "dispatch_attempted"
    DISPATCH_COMPLETED = "dispatch_completed"


# Terminal call states — no further transitions allowed
TERMINAL_CALL_STATES = frozenset({
    CallState.COMPLETED,
    CallState.FAILED,
    CallState.CANCELLED,
})

# Active call states — safety reservation must be held
ACTIVE_CALL_STATES = frozenset({
    CallState.RESERVED,
    CallState.INITIATED,
    CallState.RINGING,
    CallState.ANSWERED,
    CallState.CONNECTED,
})

# Valid agent-state transitions: (from_state, to_state)
VALID_AGENT_TRANSITIONS: frozenset[tuple[AgentState, AgentState]] = frozenset({
    (AgentState.OFFLINE, AgentState.AVAILABLE),
    (AgentState.AVAILABLE, AgentState.RESERVED),
    (AgentState.AVAILABLE, AgentState.PAUSED),
    (AgentState.AVAILABLE, AgentState.OFFLINE),
    (AgentState.RESERVED, AgentState.DIALING),
    (AgentState.RESERVED, AgentState.AVAILABLE),   # safe pre-initiation rollback only
    (AgentState.DIALING, AgentState.CONNECTED),
    (AgentState.DIALING, AgentState.WRAP_UP),       # call failed/completed without answer
    (AgentState.CONNECTED, AgentState.WRAP_UP),
    (AgentState.WRAP_UP, AgentState.AVAILABLE),
    (AgentState.PAUSED, AgentState.AVAILABLE),
    (AgentState.PAUSED, AgentState.OFFLINE),
})

# Valid call-state transitions: (from_state, to_state)
VALID_CALL_TRANSITIONS: frozenset[tuple[CallState, CallState]] = frozenset({
    (CallState.QUEUED, CallState.RESERVED),
    (CallState.RESERVED, CallState.INITIATED),
    (CallState.RESERVED, CallState.FAILED),      # definitively failed before initiation
    (CallState.RESERVED, CallState.CANCELLED),
    (CallState.INITIATED, CallState.RINGING),
    (CallState.INITIATED, CallState.ANSWERED),   # provider skips ringing event
    (CallState.INITIATED, CallState.FAILED),
    (CallState.INITIATED, CallState.COMPLETED),  # provider skips intermediate events
    (CallState.INITIATED, CallState.CANCELLED),
    (CallState.RINGING, CallState.ANSWERED),
    (CallState.RINGING, CallState.FAILED),
    (CallState.RINGING, CallState.COMPLETED),    # no_answer resolved to completed
    (CallState.RINGING, CallState.CANCELLED),
    (CallState.ANSWERED, CallState.CONNECTED),
    (CallState.ANSWERED, CallState.COMPLETED),
    (CallState.ANSWERED, CallState.FAILED),
    (CallState.CONNECTED, CallState.COMPLETED),
    (CallState.CONNECTED, CallState.FAILED),
})
