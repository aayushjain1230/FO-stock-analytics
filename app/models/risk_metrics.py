"""Descriptive risk-adjusted metric outputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class RiskMetricWindow:
    """Metrics for one historical lookback window."""

    label: str
    sample_size: int
    sharpe: float | None
    sortino: float | None
    max_drawdown: float | None
    downside_deviation: float | None
    positive_period_consistency: float | None
    recovery_time_days: int | None
    tail_loss_95: float | None


@dataclass(frozen=True)
class RiskMetricsResult:
    """Descriptive risk metrics, not predictions."""

    ticker: str
    conclusion: str
    confidence: str
    windows: list[RiskMetricWindow]
    warnings: list[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-safe metrics."""
        return {
            "ticker": self.ticker,
            "conclusion": self.conclusion,
            "confidence": self.confidence,
            "windows": [window.__dict__ for window in self.windows],
            "warnings": self.warnings,
            "generated_at": self.generated_at.isoformat(),
        }
