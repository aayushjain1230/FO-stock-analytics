"""Pairs trading engine adapters."""

from __future__ import annotations

from typing import Any, Dict


def pairs_brief(pair: Dict[str, Any]) -> Dict[str, Any]:
    """Explain one pair-trading candidate."""
    return {
        "pair": pair.get("pair"),
        "z_score": pair.get("spread_zscore"),
        "hedge_ratio": pair.get("hedge_ratio"),
        "half_life": pair.get("half_life_days"),
        "signal": pair.get("signal", {}).get("action", "watch"),
        "false_cointegration_warning": "Cointegration can fail after structural breaks or sector regime shifts.",
    }
