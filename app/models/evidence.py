"""Evidence models for Version 1 stock conclusions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

EvidenceDirection = Literal["positive", "negative", "neutral", "unknown"]
EvidenceImportance = Literal["low", "medium", "high"]
EvidenceKind = Literal["fact", "calculation", "inference", "unknown"]


@dataclass(frozen=True)
class Evidence:
    """One observable or inferred reason behind a stock conclusion."""

    signal: str
    category: str
    observed_value: Any
    interpretation: str
    direction: EvidenceDirection
    importance: EvidenceImportance
    source: str
    observed_at: datetime
    freshness: str
    kind: EvidenceKind = "fact"
    calculation: str | None = None
    model_version: str | None = None
    confidence: str | None = None
    validation_status: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly evidence record."""
        return {
            "signal": self.signal,
            "category": self.category,
            "observed_value": self.observed_value,
            "interpretation": self.interpretation,
            "direction": self.direction,
            "importance": self.importance,
            "source": self.source,
            "observed_at": self.observed_at.isoformat(),
            "freshness": self.freshness,
            "kind": self.kind,
            "calculation": self.calculation,
            "model_version": self.model_version,
            "confidence": self.confidence,
            "validation_status": self.validation_status,
        }
