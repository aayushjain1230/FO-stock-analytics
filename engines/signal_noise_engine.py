"""Signal-vs-noise diagnostics for quant research."""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable


def signal_reliability(sample_size: int, win_rate: float, average_edge: float, cost_drag: float) -> Dict[str, Any]:
    """Detect weak samples, cost fragility, and likely noisy signals."""
    warnings: list[str] = []
    if sample_size < 30:
        warnings.append("Sample size is too small for strong statistical confidence.")
    if abs(average_edge) <= cost_drag:
        warnings.append("Most or all edge disappears after estimated transaction costs.")
    if 0.45 <= win_rate <= 0.55 and abs(average_edge) < 0.01:
        warnings.append("Win rate and edge are close to noise.")
    score = min(100, max(0, sample_size / 100 * 40 + abs(average_edge) * 2000 + max(0, win_rate - 0.5) * 80))
    return {
        "score": round(score, 2),
        "classification": "Promising" if score >= 70 and not warnings else "Needs validation" if score >= 45 else "Noisy",
        "warnings": warnings,
        "explanation": (
            f"Signal score is {score:.1f}. A useful signal must survive costs, have enough observations, "
            "and show stable edge out of sample."
        ),
    }


def false_discovery_warning(strategy_count: int, best_p_value: float) -> Dict[str, Any]:
    """Warn when many tested strategies make the best result less impressive."""
    adjusted = min(1.0, best_p_value * max(strategy_count, 1))
    return {
        "raw_best_p_value": best_p_value,
        "bonferroni_adjusted_p_value": adjusted,
        "warning": adjusted > 0.05,
        "explanation": "Testing many strategy variants increases the chance that the best backtest is a false discovery.",
    }


def overfitting_diagnostics(train_metric: float, test_metric: float) -> Dict[str, Any]:
    """Compare train vs test performance to detect overfitting."""
    gap = train_metric - test_metric
    severity = "High" if gap > abs(train_metric) * 0.5 else "Medium" if gap > abs(train_metric) * 0.25 else "Low"
    return {
        "train_metric": train_metric,
        "test_metric": test_metric,
        "generalization_gap": gap,
        "overfitting_risk": severity,
        "explanation": "Large train/test gaps suggest the strategy may be fitting noise instead of signal.",
    }
