"""Volatility model contracts."""

from __future__ import annotations

from typing import Any, Dict


def volatility_forecast_contract(model: str, forecast_volatility: float, current_volatility: float) -> Dict[str, Any]:
    """Package volatility forecast with interpretation."""
    direction = "rising" if forecast_volatility > current_volatility else "falling"
    return {
        "model": model,
        "forecast_volatility": forecast_volatility,
        "current_volatility": current_volatility,
        "interpretation": f"Volatility is forecast to be {direction}; validate with regime and options data.",
    }
