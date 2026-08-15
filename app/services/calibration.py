"""Walk-forward calibration for bootstrap forecasts."""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.models.simulation import CalibrationSummary, SimulationConfig


def walk_forward_calibration(ticker: str, history: pd.DataFrame, config: SimulationConfig | None = None, step: int = 10) -> CalibrationSummary:
    """Evaluate interval and directional calibration without look-ahead leakage."""
    config = config or SimulationConfig(simulations=500, random_seed=7)
    if history is None or history.empty or "Close" not in history:
        return _empty(ticker, config.horizon_days, "No price history was available.")
    close = pd.to_numeric(history["Close"], errors="coerce").dropna()
    returns = close.pct_change().dropna()
    records: list[dict[str, float | bool]] = []
    rng = np.random.default_rng(config.random_seed)
    for end in range(config.minimum_history, len(returns) - config.horizon_days, step):
        train = returns.iloc[:end].to_numpy()
        current_price = float(close.iloc[end])
        future_price = float(close.iloc[end + config.horizon_days])
        sampled = rng.choice(train, size=(config.simulations, config.horizon_days), replace=True)
        ending = current_price * np.prod(1 + sampled, axis=1)
        actual_return = future_price / current_price - 1
        prob_up = float((ending > current_price).mean())
        records.append(
            {
                "in_50": np.percentile(ending, 25) <= future_price <= np.percentile(ending, 75),
                "in_80": np.percentile(ending, 10) <= future_price <= np.percentile(ending, 90),
                "in_90": np.percentile(ending, 5) <= future_price <= np.percentile(ending, 95),
                "direction_hit": (prob_up >= 0.5) == (actual_return >= 0),
                "brier": (prob_up - (1.0 if actual_return >= 0 else 0.0)) ** 2,
                "median_error": abs(float(np.median(ending)) - future_price) / current_price,
                "false_confident": abs(prob_up - 0.5) >= 0.25 and ((prob_up >= 0.5) != (actual_return >= 0)),
                "baseline_hit": actual_return >= 0,
            }
        )
    if not records:
        return _empty(ticker, config.horizon_days, "Not enough history for walk-forward validation.")
    df = pd.DataFrame(records)
    coverage_error = abs(float(df["in_80"].mean()) - 0.8)
    warning = "Small validation sample; treat calibration as provisional." if len(df) < 30 else None
    return CalibrationSummary(
        ticker=ticker,
        horizon_days=config.horizon_days,
        sample_size=len(df),
        coverage_50=float(df["in_50"].mean()),
        coverage_80=float(df["in_80"].mean()),
        coverage_90=float(df["in_90"].mean()),
        directional_hit_rate=float(df["direction_hit"].mean()),
        brier_score=float(df["brier"].mean()),
        calibration_error=coverage_error,
        median_absolute_forecast_error=float(df["median_error"].median()),
        false_confidence_rate=float(df["false_confident"].mean()),
        baseline_directional_hit_rate=float(df["baseline_hit"].mean()),
        warning=warning,
    )


def _empty(ticker: str, horizon_days: int, warning: str) -> CalibrationSummary:
    return CalibrationSummary(ticker, horizon_days, 0, None, None, None, None, None, None, None, None, None, warning)
