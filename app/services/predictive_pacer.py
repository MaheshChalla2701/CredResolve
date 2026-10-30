"""Predictive Pacing Engine — advisory dial-count estimation.

IMPORTANT BOUNDARY:
  - This module MUST NOT import ProviderAdapter or any provider client.
  - Its output is advisory only. The Safety Controller makes the final decision.
  - It may read agent/call counts from the database (via session), but
    cannot act on them without Safety Controller approval.

Pacing algorithm (rule-based, not ML):

  1. Maintain a rolling EWMA answer-rate estimate (configurable alpha),
     rebuilt from the campaign's recent finished calls (refresh_answer_rate).
     Prior: Beta(2, 2) — weakly informative, avoids degenerate estimates.

  2. Count outstanding unconnected calls (reserved + initiated + ringing +
     answered). In the strict reservation model each one already holds its
     own RESERVED/DIALING agent, so its answer cannot take an AVAILABLE agent.
     Only outstanding calls beyond those agents ("unbacked") compete for the
     available agents:
       unbacked = max(0, outstanding - (reserved + dialing agents))
       expected_from_outstanding = unbacked * answer_rate_estimate

  3. Count verified available agents (from DB).

  4. Desired calls = those needed to achieve utilization_target of the
     available agents, minus expected answers that will claim them:
       answers_needed = available * utilization_target - expected_from_outstanding
       raw_dials      = answers_needed / answer_rate_estimate

  5. Conservative adjustment: if the estimate has LOW confidence, halve the
     proposal. If provider health is degraded, scale by the health factor.

  6. Clamp to [0, available_agents] and to the campaign's concurrency
     headroom, then submit to the Safety Controller.

Not used — and why: call durations and setup latency. Every call reserves an
AVAILABLE agent before it is dialed (CallAllocator), so the pacer can only
choose how many of the currently free agents to use. Durations and setup
latency matter when dialing ahead for agents that will free up soon, which
needs calls placed without a reserved agent (and abandonment handling); this
design deliberately does not allow that, and the Safety Controller's strict
mode would reject it. pacer_lookahead_seconds is therefore not applied.

Why the Safety Controller must still approve:
  - The estimate is probabilistic. It can be wrong (especially for sudden
    answer-rate changes).
  - The Safety Controller reads fresh authoritative state after the estimate.
  - The pacing engine cannot bypass hard concurrency limits even if its
    estimate is "correct."

Document: a low answer rate does NOT justify unsafe over-dialing.
  If answer_rate = 0.10, and we dial 10 calls per available agent,
  we expect 1 to answer — but with high variance. We could get 3 answers
  with only 1 available agent, causing 2 abandoned connected calls.
  The Safety Controller in strict mode prevents this.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from app.core.config import get_settings
from app.core.enums import AgentState, CallState
from app.core.logging import get_logger
from app.core.metrics import calls_proposed

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
_settings = get_settings()


@dataclass
class PacingRecommendation:
    """Advisory output from the pacing engine."""
    campaign_id: str
    recommended_count: int
    answer_rate_estimate: float
    answer_rate_confidence: str  # LOW / MEDIUM / HIGH
    outstanding_calls: int
    expected_answers_from_outstanding: float
    available_agents: int
    provider_health_factor: float
    calculation_notes: list[str] = field(default_factory=list)
    recommended_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class AnswerRateEstimator:
    """EWMA-based answer rate estimator with Beta-Binomial prior.

    The Beta(alpha_prior, beta_prior) prior represents initial uncertainty.
    With each observed outcome, we update using EWMA with smoothing_alpha.

    Important: this uses recent observations, not all-time history.
    A sudden answer-rate change will propagate within ~1/alpha cycles.
    """

    def __init__(
        self,
        smoothing_alpha: float | None = None,
        alpha_prior: float = 2.0,
        beta_prior: float = 2.0,
        min_samples: int | None = None,
    ) -> None:
        self.alpha = smoothing_alpha if smoothing_alpha is not None else _settings.pacer_smoothing_alpha
        self.min_samples = min_samples if min_samples is not None else _settings.pacer_min_samples
        # Beta-Binomial prior
        self._alpha_prior = alpha_prior
        self._beta_prior = beta_prior
        # Effective sample count for EWMA decay
        self._ewma_estimate = alpha_prior / (alpha_prior + beta_prior)
        self._sample_count = 0

    def observe(self, answered: bool) -> None:
        """Update estimate with one new observation."""
        outcome = 1.0 if answered else 0.0
        self._ewma_estimate = (
            self.alpha * outcome + (1 - self.alpha) * self._ewma_estimate
        )
        self._sample_count += 1

    def observe_batch(self, answered: int, total: int) -> None:
        """Update with a batch of observations."""
        for i in range(total):
            self.observe(i < answered)

    @property
    def estimate(self) -> float:
        return max(0.01, min(0.99, self._ewma_estimate))

    @property
    def confidence(self) -> str:
        if self._sample_count < self.min_samples:
            return "LOW"
        elif self._sample_count < self.min_samples * 5:
            return "MEDIUM"
        return "HIGH"

    @property
    def sample_count(self) -> int:
        return self._sample_count


class PredictivePacingEngine:
    """Rule-based predictive pacing engine.

    Produces advisory recommendations that MUST be validated by
    the SafetyController before any call can be initiated.

    This class has NO access to provider clients. It only reads
    agent/call state from the database.
    """

    def __init__(
        self,
        estimator: AnswerRateEstimator | None = None,
        utilization_target: float | None = None,
    ) -> None:
        self.estimator = estimator or AnswerRateEstimator()
        self.utilization_target = (
            utilization_target
            if utilization_target is not None
            else _settings.pacer_utilization_target
        )

    async def refresh_answer_rate(
        self, session: "AsyncSession", campaign_id: str, *, window: int = 100
    ) -> int:
        """Rebuild the answer-rate estimate from the campaign's most recent finished
        calls that the provider actually placed (oldest first, so the EWMA weights
        recent outcomes most). Returns the number of outcomes observed."""
        from sqlalchemy import select
        from app.core.enums import TERMINAL_CALL_STATES
        from app.persistence.models import Call

        result = await session.execute(
            select(Call.answered_at, Call.state)
            .where(
                Call.campaign_id == campaign_id,
                Call.state.in_(list(TERMINAL_CALL_STATES)),
                Call.provider_call_id.is_not(None),
            )
            .order_by(Call.completed_at.desc())
            .limit(window)
        )
        outcomes = [
            answered_at is not None or state == CallState.COMPLETED
            for answered_at, state in result.all()
        ]
        self.estimator = AnswerRateEstimator(
            smoothing_alpha=self.estimator.alpha,
            min_samples=self.estimator.min_samples,
        )
        for answered in reversed(outcomes):
            self.estimator.observe(answered)
        return len(outcomes)

    async def recommend(
        self,
        session: "AsyncSession",
        campaign_id: str,
        provider_id: str,
        *,
        provider_health_factor: float = 1.0,  # 0.0 = degraded, 1.0 = healthy
    ) -> PacingRecommendation:
        """Produce an advisory dial count recommendation.

        provider_health_factor: caller-provided health multiplier (0.0–1.0).
        A degraded provider reduces the recommendation conservatively.
        """
        from app.persistence.repositories import AgentRepository, CallRepository

        agent_repo = AgentRepository(session)
        call_repo = CallRepository(session)

        # Read fresh state
        state_counts = await agent_repo.count_by_state(campaign_id)
        available = state_counts.get(AgentState.AVAILABLE, 0)
        reserved = state_counts.get(AgentState.RESERVED, 0)
        dialing = state_counts.get(AgentState.DIALING, 0)
        connected = state_counts.get(AgentState.CONNECTED, 0)
        wrap_up = state_counts.get(AgentState.WRAP_UP, 0)

        # Outstanding calls = those that might still answer
        active_calls = await call_repo.count_active(campaign_id)
        # Calls that are pre-connect (can still answer)
        from sqlalchemy import select, func
        from app.persistence.models import Call
        unconnected_states = [
            CallState.RESERVED, CallState.INITIATED,
            CallState.RINGING, CallState.ANSWERED
        ]
        result = await session.execute(
            select(func.count(Call.id)).where(
                Call.campaign_id == campaign_id,
                Call.state.in_(unconnected_states),
            )
        )
        outstanding = result.scalar_one() or 0

        answer_rate = self.estimator.estimate
        confidence = self.estimator.confidence

        notes = []

        # Expected answers from outstanding calls that would need one of the
        # available agents (calls without their own RESERVED/DIALING agent).
        unbacked = max(0, outstanding - (reserved + dialing))
        expected_from_outstanding = unbacked * answer_rate
        notes.append(
            f"outstanding={outstanding} (unbacked={unbacked}) × answer_rate={answer_rate:.3f} "
            f"= expected_outstanding_answers={expected_from_outstanding:.2f}"
        )

        # Target utilization: we want (utilization_target × available agents) to be connected
        target_connected = available * self.utilization_target
        notes.append(f"target_connected={target_connected:.2f} (target={self.utilization_target})")

        # Additional answers we need to start now to reach target. Agents already
        # CONNECTED are not in `available`, so they are not subtracted here.
        answers_needed = max(0.0, target_connected - expected_from_outstanding)
        notes.append(f"answers_needed={answers_needed:.2f}")

        # How many new dials to generate `answers_needed` answers
        if answer_rate > 0.001:
            raw_dials = answers_needed / answer_rate
        else:
            raw_dials = 0.0
        notes.append(f"raw_dials_for_target={raw_dials:.2f}")

        # Conservative: use floor, not ceil — don't round up speculatively
        proposed = int(math.floor(raw_dials))

        # Apply provider health factor
        if provider_health_factor < 1.0:
            original = proposed
            proposed = int(math.floor(proposed * provider_health_factor))
            notes.append(
                f"provider_health_factor={provider_health_factor:.2f} "
                f"reduced {original} → {proposed}"
            )

        # If confidence is LOW, be more conservative
        if confidence == "LOW":
            original = proposed
            proposed = max(0, proposed // 2)
            notes.append(
                f"LOW confidence: halved recommendation {original} → {proposed}"
            )

        # Cap at available agents (hard cap before handing to safety controller)
        proposed = min(proposed, available)

        # NEVER propose more than max campaign concurrency
        # (safety controller re-checks, but avoid obviously bad proposals)
        from app.persistence.repositories import CampaignRepository
        campaign_repo = CampaignRepository(session)
        campaign = await campaign_repo.get(campaign_id)
        if campaign:
            max_cap = campaign.max_concurrent_calls - active_calls
            proposed = min(proposed, max(0, max_cap))

        calls_proposed.labels(campaign_id=campaign_id, mode="predictive").inc(proposed)

        rec = PacingRecommendation(
            campaign_id=campaign_id,
            recommended_count=proposed,
            answer_rate_estimate=answer_rate,
            answer_rate_confidence=confidence,
            outstanding_calls=outstanding,
            expected_answers_from_outstanding=expected_from_outstanding,
            available_agents=available,
            provider_health_factor=provider_health_factor,
            calculation_notes=notes,
        )

        logger.info(
            "predictive_recommendation",
            campaign_id=campaign_id,
            recommended=proposed,
            answer_rate=round(answer_rate, 4),
            confidence=confidence,
            outstanding=outstanding,
            available=available,
            connected=connected,
            wrap_up=wrap_up,
            notes=notes,
        )

        return rec

    def record_outcome(self, answered: bool) -> None:
        """Feed a call outcome back into the answer-rate estimator."""
        self.estimator.observe(answered)

    def record_batch_outcome(self, answered: int, total: int) -> None:
        self.estimator.observe_batch(answered, total)
