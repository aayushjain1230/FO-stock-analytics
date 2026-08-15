"""Relative-value and pair-analysis result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from app.models.evidence import Evidence

RelationshipStatus = Literal[
    "Normal Relationship",
    "Moderate Divergence",
    "Unusual Divergence",
    "Relationship Weakening",
    "Relationship Broken",
    "Insufficient Evidence",
]


@dataclass(frozen=True)
class PeerCandidate:
    """Economically defensible pair candidate."""

    ticker_a: str
    ticker_b: str
    reasons: list[str]
    confidence: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RelativeValueResult:
    """Validated relative-value relationship result."""

    ticker_a: str
    ticker_b: str
    relationship_status: RelationshipStatus
    divergence_direction: str
    spread_zscore: float | None
    hedge_ratio: float | None
    half_life_days: float | None
    raw_pvalue: float | None
    adjusted_pvalue: float | None
    rolling_stability: float | None
    out_of_sample_status: str
    structural_break_status: str
    estimated_cost_warning: str | None
    confidence: str
    evidence: list[Evidence]
    contradicting_evidence: list[Evidence]
    limitations: list[str]
    valid_until: datetime | None
    analyzed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def pair_label(self) -> str:
        """Return display label."""
        return f"{self.ticker_a} vs {self.ticker_b}"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe result."""
        return {
            "ticker_a": self.ticker_a,
            "ticker_b": self.ticker_b,
            "relationship_status": self.relationship_status,
            "divergence_direction": self.divergence_direction,
            "spread_zscore": self.spread_zscore,
            "hedge_ratio": self.hedge_ratio,
            "half_life_days": self.half_life_days,
            "raw_pvalue": self.raw_pvalue,
            "adjusted_pvalue": self.adjusted_pvalue,
            "rolling_stability": self.rolling_stability,
            "out_of_sample_status": self.out_of_sample_status,
            "structural_break_status": self.structural_break_status,
            "estimated_cost_warning": self.estimated_cost_warning,
            "confidence": self.confidence,
            "evidence": [item.to_dict() for item in self.evidence],
            "contradicting_evidence": [item.to_dict() for item in self.contradicting_evidence],
            "limitations": self.limitations,
            "valid_until": self.valid_until.isoformat() if self.valid_until else None,
            "analyzed_at": self.analyzed_at.isoformat(),
        }


def default_valid_until(days: int = 20) -> datetime:
    """Return a conservative revalidation deadline."""
    return datetime.now(timezone.utc) + timedelta(days=days)
