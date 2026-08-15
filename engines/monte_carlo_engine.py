"""Monte Carlo research engine contracts."""

from __future__ import annotations

from typing import Any, Dict


def simulation_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Explain existing Monte Carlo simulation output."""
    return {
        "question_answered": "What range of outcomes should I prepare for?",
        "expected_return": payload.get("expected_return"),
        "probability_of_loss": payload.get("probability_of_loss"),
        "probability_of_large_drawdown": payload.get("probability_of_large_drawdown"),
        "interpretation": "Monte Carlo is useful for distribution awareness, not precise prediction.",
    }
