"""Options research engine contracts."""

from __future__ import annotations

from typing import Dict


def breakeven_call(strike: float, premium: float) -> Dict[str, float | str]:
    """Calculate call option breakeven."""
    return {"breakeven": strike + premium, "formula_used": "call breakeven = strike + premium"}


def breakeven_put(strike: float, premium: float) -> Dict[str, float | str]:
    """Calculate put option breakeven."""
    return {"breakeven": strike - premium, "formula_used": "put breakeven = strike - premium"}
