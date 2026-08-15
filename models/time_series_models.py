"""Time-series model contracts."""

from __future__ import annotations

from typing import Any, Dict


def forecast_contract(model: str, forecast: float, confidence_interval: tuple[float, float]) -> Dict[str, Any]:
    """Package a forecast with uncertainty."""
    return {
        "model": model,
        "forecast": forecast,
        "confidence_interval": confidence_interval,
        "interpretation": "Forecast should be evaluated against uncertainty bands and regime assumptions.",
    }
