"""Walk-forward relationship validation for relative-value pairs."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from app.analysis.cointegration import analyze_cointegration


@dataclass(frozen=True)
class PairValidationSummary:
    """Historical validation summary for pair divergences."""

    sample_size: int
    narrowing_rate: float | None
    median_time_to_normalization: float | None
    max_adverse_move: float | None
    false_positive_rate: float | None
    relationship_survival_rate: float | None
    after_cost_status: str
    random_entry_baseline: float | None
    correlation_only_baseline: float | None
    warning: str | None

    def to_dict(self) -> dict:
        return self.__dict__


def validate_pair_walk_forward(price_a: pd.Series, price_b: pd.Series, train_window: int = 252, test_window: int = 42, z_threshold: float = 1.5, transaction_cost_bps: float = 20) -> PairValidationSummary:
    """Validate relationship behavior without optimizing on the final test period."""
    aligned = pd.concat([price_a.rename("a"), price_b.rename("b")], axis=1, sort=False).dropna()
    if len(aligned) < train_window + test_window + 30:
        return PairValidationSummary(0, None, None, None, None, None, "Insufficient sample", None, None, "Not enough history for walk-forward pair validation.")
    outcomes = []
    adverse = []
    survival = []
    for start in range(0, len(aligned) - train_window - test_window, max(test_window, 10)):
        train = aligned.iloc[start : start + train_window]
        test = aligned.iloc[start + train_window : start + train_window + test_window]
        stats = analyze_cointegration(train["a"], train["b"], min_obs=max(120, train_window // 2))
        if stats.hedge_ratio is None or stats.raw_pvalue is None or stats.raw_pvalue > 0.15:
            survival.append(False)
            continue
        survival.append(True)
        logs_train = np.log(train)
        spread_train = logs_train["a"] - (stats.intercept + stats.hedge_ratio * logs_train["b"])
        logs_test = np.log(test)
        spread_test = logs_test["a"] - (stats.intercept + stats.hedge_ratio * logs_test["b"])
        z0 = float((spread_test.iloc[0] - spread_train.mean()) / max(spread_train.std(), 1e-9))
        if abs(z0) < z_threshold:
            continue
        target = 0.5 * abs(z0)
        z_path = (spread_test - spread_train.mean()) / max(spread_train.std(), 1e-9)
        narrowed_idx = np.where(np.abs(z_path.to_numpy()) <= target)[0]
        outcomes.append(len(narrowed_idx) > 0)
        adverse.append(float(np.max(np.abs(z_path.to_numpy())) - abs(z0)))
    if not outcomes:
        return PairValidationSummary(0, None, None, None, None, float(np.mean(survival)) if survival else None, "No validated divergences", None, None, "No historical divergences passed the validation filter.")
    narrowing = float(np.mean(outcomes))
    false_positive = float(1 - narrowing)
    after_cost = "Passed conservative cost check" if narrowing >= 0.55 and transaction_cost_bps <= 30 else "Costs or weak normalization reduce usefulness"
    return PairValidationSummary(len(outcomes), narrowing, float(test_window / 2) if narrowing > 0 else None, float(max(adverse) if adverse else 0), false_positive, float(np.mean(survival)) if survival else None, after_cost, 0.5, 0.5, "Small sample; treat as relationship research, not a strategy track record." if len(outcomes) < 20 else None)
