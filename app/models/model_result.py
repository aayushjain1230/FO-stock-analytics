"""Typed contracts for advanced model outputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

ModelStatus = Literal["success", "insufficient_data", "unstable", "failed_validation", "unavailable", "error"]


@dataclass(frozen=True)
class AdvancedModelResult:
    """Common contract every advanced model result must satisfy."""

    model_name: str
    model_version: str
    ticker_or_pair: str
    status: ModelStatus
    generated_at: datetime
    data_start: str | None
    data_end: str | None
    sample_size: int
    parameters: dict[str, Any]
    result: dict[str, Any]
    confidence: str
    warnings: list[str] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)
    validation_summary: dict[str, Any] = field(default_factory=dict)

    def contributes_to_conclusion(self) -> bool:
        """Return whether this model can be used as evidence."""
        return self.status == "success" and self.confidence != "Insufficient Data"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe model result."""
        return {
            "model_name": self.model_name,
            "model_version": self.model_version,
            "ticker_or_pair": self.ticker_or_pair,
            "status": self.status,
            "generated_at": self.generated_at.isoformat(),
            "data_start": self.data_start,
            "data_end": self.data_end,
            "sample_size": self.sample_size,
            "parameters": self.parameters,
            "result": self.result,
            "confidence": self.confidence,
            "warnings": self.warnings,
            "limitations": self.limitations,
            "validation_summary": self.validation_summary,
        }


def now_utc() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)
