"""Single-stock factor model outputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class FactorModelResult:
    """Single-stock factor analysis, not portfolio exposure."""

    ticker: str
    conclusion: str
    market_sensitivity: float | None
    sector_sensitivity: float | None
    momentum_exposure: float | None
    explained_variation: float | None
    residual_interpretation: str
    rolling_stability: float | None
    confidence: str
    sample_size: int
    warnings: list[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-safe factor result."""
        return {
            "ticker": self.ticker,
            "conclusion": self.conclusion,
            "market_sensitivity": self.market_sensitivity,
            "sector_sensitivity": self.sector_sensitivity,
            "momentum_exposure": self.momentum_exposure,
            "explained_variation": self.explained_variation,
            "residual_interpretation": self.residual_interpretation,
            "rolling_stability": self.rolling_stability,
            "confidence": self.confidence,
            "sample_size": self.sample_size,
            "warnings": self.warnings,
            "generated_at": self.generated_at.isoformat(),
        }
