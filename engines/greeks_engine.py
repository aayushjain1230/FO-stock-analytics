"""Options Greeks explanation helpers."""

from __future__ import annotations

from typing import Dict


def greeks_interpretation(delta: float, gamma: float, theta: float, vega: float) -> Dict[str, float | str]:
    """Explain a Greeks snapshot."""
    return {
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "interpretation": "Delta is direction, gamma is convexity, theta is time decay, and vega is volatility sensitivity.",
    }
