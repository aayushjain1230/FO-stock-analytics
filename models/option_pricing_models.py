"""Option-pricing model contracts."""

from __future__ import annotations

from typing import Dict


def option_price_contract(model: str, fair_value: float, market_price: float) -> Dict[str, object]:
    """Package option fair-value output."""
    edge = fair_value - market_price
    return {"model": model, "fair_value": fair_value, "market_price": market_price, "edge": edge}
