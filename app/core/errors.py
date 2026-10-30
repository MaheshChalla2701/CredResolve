"""Domain-specific exception hierarchy."""
from __future__ import annotations


class SmartDialerError(Exception):
    """Base for all application errors."""


class InvalidStateTransitionError(SmartDialerError):
    """Raised when a state machine transition is not allowed."""

    def __init__(self, entity: str, from_state: str, to_state: str, reason: str = "") -> None:
        self.entity = entity
        self.from_state = from_state
        self.to_state = to_state
        self.reason = reason
        msg = f"{entity}: transition {from_state!r} -> {to_state!r} is not allowed"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)


class AllocationError(SmartDialerError):
    """Raised when atomic allocation fails."""


class NoAvailableAgentError(AllocationError):
    """No eligible agent is available for allocation."""


class NoEligibleBorrowerError(AllocationError):
    """No eligible borrower is queued for the campaign."""


class DuplicateAllocationError(AllocationError):
    """A duplicate allocation was attempted and rejected."""


class CampaignConcurrencyLimitError(AllocationError):
    """The campaign already has max_concurrent_calls active calls."""


class SafetyViolationError(SmartDialerError):
    """An operation was blocked by the Safety Controller."""


class ProviderError(SmartDialerError):
    """Error communicating with a telephony provider."""


class AmbiguousProviderOutcomeError(ProviderError):
    """Provider outcome is uncertain (timeout after possible acceptance)."""


class CircuitBreakerOpenError(ProviderError):
    """Provider circuit breaker is OPEN; requests are blocked."""


class IdempotencyKeyConflictError(SmartDialerError):
    """An idempotency key was reused with different parameters."""


class ReconciliationError(SmartDialerError):
    """Error during reconciliation."""


class CampaignNotActiveError(SmartDialerError):
    """Campaign is not in ACTIVE state."""


class AgentNotEligibleError(SmartDialerError):
    """Agent is not eligible for the requested operation."""


class StaleUpdateError(SmartDialerError):
    """A conditional update was rejected because state is stale."""
