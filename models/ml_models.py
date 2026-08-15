"""Machine-learning model output contracts."""

from __future__ import annotations

from typing import Any, Dict


def model_leaderboard_row(name: str, accuracy: float | None = None, sharpe: float | None = None, drawdown: float | None = None) -> Dict[str, Any]:
    """Create one model-comparison row."""
    return {"model": name, "accuracy": accuracy, "sharpe": sharpe, "drawdown": drawdown}
