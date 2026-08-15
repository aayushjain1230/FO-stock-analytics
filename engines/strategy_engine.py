"""Strategy testing engine contracts."""

from __future__ import annotations

from typing import Any, Dict

from engines.signal_noise_engine import signal_reliability


def strategy_validation_summary(backtest: Dict[str, Any]) -> Dict[str, Any]:
    """Explain whether a strategy backtest is research-useful."""
    performance = backtest.get("performance", {})
    robustness = backtest.get("robustness", {})
    sample_size = int(robustness.get("sample_size", 0) or 0)
    edge = float(performance.get("cagr", 0.0) or 0.0)
    reliability = signal_reliability(sample_size, robustness.get("positive_fold_pct", 0.5) or 0.5, edge, 0.006)
    return {
        "performance": performance,
        "robustness": robustness,
        "signal_reliability": reliability,
        "interpretation": "A strategy is only interesting if it survives costs and out-of-sample validation.",
    }
