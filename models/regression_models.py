"""Regression model contracts and diagnostics."""

from __future__ import annotations

from typing import Any, Dict


def regression_output_contract(model_name: str, coefficients: Dict[str, float], r_squared: float) -> Dict[str, Any]:
    """Package regression outputs with interpretation."""
    return {
        "model_name": model_name,
        "coefficients": coefficients,
        "r_squared": r_squared,
        "interpretation": "Regression coefficients estimate relationships, not causality. Check residuals and stability.",
    }
