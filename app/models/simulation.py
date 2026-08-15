"""Historical-bootstrap simulation and calibration models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

BootstrapMethod = Literal["standard", "block", "regime_conditioned"]


@dataclass(frozen=True)
class SimulationConfig:
    """Configurable bootstrap settings."""

    horizon_days: int = 30
    simulations: int = 5000
    lookback_days: int = 252
    block_length: int = 5
    random_seed: int = 42
    minimum_history: int = 90
    method: BootstrapMethod = "standard"
    model_version: str = "v2_bootstrap_1"


@dataclass(frozen=True)
class SimulationResult:
    """Scenario distribution generated from historical returns."""

    ticker: str
    method: BootstrapMethod
    horizon_days: int
    simulations: int
    current_price: float | None
    median_ending_price: float | None
    percentile_range: dict[str, float]
    scenarios_ending_higher_pct: float | None
    falling_more_than_5_pct: float | None
    falling_more_than_10_pct: float | None
    gaining_more_than_5_pct: float | None
    gaining_more_than_10_pct: float | None
    max_drawdown_percentiles: dict[str, float]
    model_disagreement: str
    data_coverage: str
    model_outlook: str
    risk_level: str
    confidence: str
    event_warning: str | None
    explanation: str
    limitations: list[str]
    calibration_sample_size: int | None = None
    model_version: str = "v2_bootstrap_1"
    simulated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe result."""
        return {
            "ticker": self.ticker,
            "method": self.method,
            "horizon_days": self.horizon_days,
            "simulations": self.simulations,
            "current_price": self.current_price,
            "median_ending_price": self.median_ending_price,
            "percentile_range": self.percentile_range,
            "scenarios_ending_higher_pct": self.scenarios_ending_higher_pct,
            "falling_more_than_5_pct": self.falling_more_than_5_pct,
            "falling_more_than_10_pct": self.falling_more_than_10_pct,
            "gaining_more_than_5_pct": self.gaining_more_than_5_pct,
            "gaining_more_than_10_pct": self.gaining_more_than_10_pct,
            "max_drawdown_percentiles": self.max_drawdown_percentiles,
            "model_disagreement": self.model_disagreement,
            "data_coverage": self.data_coverage,
            "model_outlook": self.model_outlook,
            "risk_level": self.risk_level,
            "confidence": self.confidence,
            "event_warning": self.event_warning,
            "explanation": self.explanation,
            "limitations": self.limitations,
            "calibration_sample_size": self.calibration_sample_size,
            "model_version": self.model_version,
            "simulated_at": self.simulated_at.isoformat(),
        }


@dataclass(frozen=True)
class CalibrationSummary:
    """Walk-forward validation summary for scenario forecasts."""

    ticker: str
    horizon_days: int
    sample_size: int
    coverage_50: float | None
    coverage_80: float | None
    coverage_90: float | None
    directional_hit_rate: float | None
    brier_score: float | None
    calibration_error: float | None
    median_absolute_forecast_error: float | None
    false_confidence_rate: float | None
    baseline_directional_hit_rate: float | None
    warning: str | None
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe result."""
        return {
            "ticker": self.ticker,
            "horizon_days": self.horizon_days,
            "sample_size": self.sample_size,
            "coverage_50": self.coverage_50,
            "coverage_80": self.coverage_80,
            "coverage_90": self.coverage_90,
            "directional_hit_rate": self.directional_hit_rate,
            "brier_score": self.brier_score,
            "calibration_error": self.calibration_error,
            "median_absolute_forecast_error": self.median_absolute_forecast_error,
            "false_confidence_rate": self.false_confidence_rate,
            "baseline_directional_hit_rate": self.baseline_directional_hit_rate,
            "warning": self.warning,
            "evaluated_at": self.evaluated_at.isoformat(),
        }
