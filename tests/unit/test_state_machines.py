"""Unit tests for Agent and Call state machines."""
from __future__ import annotations

import pytest

from app.core.enums import (
    VALID_AGENT_TRANSITIONS,
    VALID_CALL_TRANSITIONS,
    AgentState,
    CallState,
)
from app.core.errors import InvalidStateTransitionError
from app.domain.state_machines import (
    is_active_call_state,
    is_terminal_call_state,
    validate_agent_transition,
    validate_call_transition,
)


class TestAgentStateMachine:
    def test_valid_agent_transitions(self) -> None:
        """Every defined valid transition must succeed without error."""
        for from_s, to_s in VALID_AGENT_TRANSITIONS:
            validate_agent_transition(from_s, to_s)

    def test_invalid_agent_transitions(self) -> None:
        """Arbitrary illegal transitions must raise InvalidStateTransitionError."""
        invalid_pairs = [
            (AgentState.OFFLINE, AgentState.CONNECTED),
            (AgentState.OFFLINE, AgentState.RESERVED),
            (AgentState.CONNECTED, AgentState.AVAILABLE),
            (AgentState.CONNECTED, AgentState.OFFLINE),
            (AgentState.WRAP_UP, AgentState.CONNECTED),
            (AgentState.RESERVED, AgentState.CONNECTED),
        ]
        for from_s, to_s in invalid_pairs:
            with pytest.raises(InvalidStateTransitionError) as exc_info:
                validate_agent_transition(from_s, to_s)
            assert exc_info.value.entity == "Agent"

    def test_string_inputs_work(self) -> None:
        """Validation accepts string representations as well as enums."""
        validate_agent_transition("AVAILABLE", "RESERVED")
        with pytest.raises(InvalidStateTransitionError):
            validate_agent_transition("OFFLINE", "CONNECTED")


class TestCallStateMachine:
    def test_valid_call_transitions(self) -> None:
        """Every defined valid call transition must succeed without error."""
        for from_s, to_s in VALID_CALL_TRANSITIONS:
            validate_call_transition(from_s, to_s)

    def test_invalid_call_transitions(self) -> None:
        """Illegal call transitions must be rejected."""
        invalid_pairs = [
            (CallState.COMPLETED, CallState.RINGING),
            (CallState.COMPLETED, CallState.INITIATED),
            (CallState.FAILED, CallState.CONNECTED),
            (CallState.QUEUED, CallState.CONNECTED),
            (CallState.RINGING, CallState.QUEUED),
        ]
        for from_s, to_s in invalid_pairs:
            with pytest.raises(InvalidStateTransitionError) as exc_info:
                validate_call_transition(from_s, to_s)
            assert exc_info.value.entity == "Call"

    def test_terminal_states(self) -> None:
        """Check terminal state detection."""
        assert is_terminal_call_state(CallState.COMPLETED)
        assert is_terminal_call_state(CallState.FAILED)
        assert is_terminal_call_state(CallState.CANCELLED)
        assert not is_terminal_call_state(CallState.RINGING)
        assert not is_terminal_call_state(CallState.CONNECTED)
        assert not is_terminal_call_state(CallState.RESERVED)

    def test_active_states(self) -> None:
        """Check active state detection."""
        assert is_active_call_state(CallState.RESERVED)
        assert is_active_call_state(CallState.INITIATED)
        assert is_active_call_state(CallState.RINGING)
        assert is_active_call_state(CallState.ANSWERED)
        assert is_active_call_state(CallState.CONNECTED)
        assert not is_active_call_state(CallState.COMPLETED)
        assert not is_active_call_state(CallState.FAILED)
        assert not is_active_call_state(CallState.QUEUED)
