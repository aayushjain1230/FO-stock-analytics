"""Large-buyer activity models with cautious user-facing language."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from app.models.evidence import Evidence
from app.models.sec_filing import FilingRecord

WhaleLevel = Literal["High", "Elevated", "Normal", "Distribution Risk", "Insufficient Data"]


@dataclass(frozen=True)
class WhaleActivityResult:
    """Evidence consistent with larger-buyer or seller activity."""

    ticker: str
    level: WhaleLevel
    internal_score: float | None
    confidence: str
    evidence: list[Evidence]
    contradicting_evidence: list[Evidence]
    confounders: list[str]
    confirmed_filings: list[FilingRecord]
    inference: str
    unknowns: list[str]
    component_scores: dict[str, float | None] = field(default_factory=dict)
    analyzed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe result."""
        return {
            "ticker": self.ticker,
            "level": self.level,
            "internal_score": self.internal_score,
            "confidence": self.confidence,
            "evidence": [item.to_dict() for item in self.evidence],
            "contradicting_evidence": [item.to_dict() for item in self.contradicting_evidence],
            "confounders": self.confounders,
            "confirmed_filings": [item.to_dict() for item in self.confirmed_filings],
            "inference": self.inference,
            "unknowns": self.unknowns,
            "component_scores": self.component_scores,
            "analyzed_at": self.analyzed_at.isoformat(),
        }
