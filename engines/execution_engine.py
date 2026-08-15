"""Execution-cost and slippage research helpers."""

from __future__ import annotations

from typing import Dict


def net_edge_after_costs(gross_edge: float, commission: float, slippage: float, spread_cost: float = 0.0) -> Dict[str, float | str]:
    """Calculate edge after execution costs."""
    net = gross_edge - commission - slippage - spread_cost
    return {
        "gross_edge": gross_edge,
        "commission": commission,
        "slippage": slippage,
        "spread_cost": spread_cost,
        "net_edge": net,
        "interpretation": "A signal is not tradeable unless edge survives realistic execution costs.",
    }
