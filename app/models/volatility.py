"""Volatility forecast models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class VolatilityForecast:
    """Plain-English-ready volatility forecast."""

    ticker: str
    active_model: str
    expected_movement: str
    annualized_volatility: float | None
    horizon_volatility: float | None
    horizon_days: int
    confidence: str
    why: str
    main_limitation: str
    ewma_volatility: float | None
    garch_volatility: float | None
    garch_status: str
    validation_summary: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe forecast."""
        return {
            "ticker": self.ticker,
            "active_model": self.active_model,
            "expected_movement": self.expected_movement,
            "annualized_volatility": self.annualized_volatility,
            "horizon_volatility": self.horizon_volatility,
            "horizon_days": self.horizon_days,
            "confidence": self.confidence,
            "why": self.why,
            "main_limitation": self.main_limitation,
            "ewma_volatility": self.ewma_volatility,
            "garch_volatility": self.garch_volatility,
            "garch_status": self.garch_status,
            "validation_summary": self.validation_summary,
            "warnings": self.warnings,
            "generated_at": self.generated_at.isoformat(),
        }
