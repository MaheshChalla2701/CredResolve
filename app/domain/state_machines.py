"""Agent and Call state-machine validators.

These functions are the single authoritative source for what transitions are
allowed. They raise InvalidStateTransitionError on illegal transitions so the
error is caught before any database writes.
"""
from __future__ import annotations

from app.core.enums import (
    VALID_AGENT_TRANSITIONS,
    VALID_CALL_TRANSITIONS,
    AgentState,
    CallState,
)
from app.core.errors import InvalidStateTransitionError


def validate_agent_transition(from_state: AgentState | str, to_state: AgentState | str) -> None:
    """Raise InvalidStateTransitionError if the agent transition is not legal."""
    fs = AgentState(from_state)
    ts = AgentState(to_state)
    if (fs, ts) not in VALID_AGENT_TRANSITIONS:
        raise InvalidStateTransitionError("Agent", from_state, to_state)


def validate_call_transition(from_state: CallState | str, to_state: CallState | str) -> None:
    """Raise InvalidStateTransitionError if the call transition is not legal."""
    fs = CallState(from_state)
    ts = CallState(to_state)
    if (fs, ts) not in VALID_CALL_TRANSITIONS:
        raise InvalidStateTransitionError("Call", from_state, to_state)


def is_terminal_call_state(state: CallState | str) -> bool:
    from app.core.enums import TERMINAL_CALL_STATES
    return CallState(state) in TERMINAL_CALL_STATES


def is_active_call_state(state: CallState | str) -> bool:
    from app.core.enums import ACTIVE_CALL_STATES
    return CallState(state) in ACTIVE_CALL_STATES
