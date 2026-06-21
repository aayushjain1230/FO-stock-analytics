"""Market microstructure, order book, and execution analytics."""

from typing import Dict, Iterable, List

import numpy as np
import pandas as pd


def order_book_metrics(bids: Iterable[Dict], asks: Iterable[Dict]) -> Dict:
    bids = sorted(list(bids), key=lambda x: x.get("price", 0), reverse=True)
    asks = sorted(list(asks), key=lambda x: x.get("price", 0))
    if not bids or not asks:
        return {"available": False, "message": "Need bid and ask levels."}
    best_bid = float(bids[0]["price"])
    best_ask = float(asks[0]["price"])
    bid_size = float(bids[0].get("size", 0))
    ask_size = float(asks[0].get("size", 0))
    mid = (best_bid + best_ask) / 2
    spread = best_ask - best_bid
    bid_depth = sum(float(level.get("size", 0)) for level in bids)
    ask_depth = sum(float(level.get("size", 0)) for level in asks)
    imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth) if bid_depth + ask_depth else 0.0
    microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size) if bid_size + ask_size else mid
    return {
        "available": True,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid,
        "spread": spread,
        "spread_bps": spread / mid * 10000 if mid else None,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "order_book_imbalance": imbalance,
        "microprice": microprice,
        "interpretation": "Imbalance and microprice estimate short-term pressure inside the spread.",
    }


def vwap(trades: pd.DataFrame) -> Dict:
    if trades is None or trades.empty or not {"price", "size"}.issubset(trades.columns):
        return {"available": False, "message": "Need price and size columns."}
    prices = pd.to_numeric(trades["price"], errors="coerce")
    sizes = pd.to_numeric(trades["size"], errors="coerce")
    denom = sizes.sum()
    return {"available": True, "vwap": float((prices * sizes).sum() / denom) if denom else None, "volume": float(denom)}


def twap_schedule(total_shares: float, slices: int) -> List[Dict]:
    slices = max(int(slices), 1)
    base = total_shares / slices
    return [{"slice": i + 1, "shares": float(base)} for i in range(slices)]


def participation_schedule(total_shares: float, expected_market_volume: Iterable[float], max_participation: float = 0.10) -> List[Dict]:
    schedule = []
    remaining = float(total_shares)
    for idx, volume in enumerate(expected_market_volume):
        shares = min(remaining, float(volume) * max_participation)
        schedule.append({"slice": idx + 1, "shares": shares, "participation_rate": max_participation})
        remaining -= shares
        if remaining <= 0:
            break
    if remaining > 0:
        schedule.append({"slice": len(schedule) + 1, "shares": remaining, "participation_rate": "residual"})
    return schedule


def implementation_shortfall(decision_price: float, execution_price: float, shares: float, side: str = "buy", fees: float = 0.0) -> Dict:
    direction = 1 if side.lower() == "buy" else -1
    shortfall = direction * (execution_price - decision_price) * shares + fees
    notional = decision_price * shares
    return {
        "decision_price": decision_price,
        "execution_price": execution_price,
        "shares": shares,
        "side": side,
        "shortfall_dollars": float(shortfall),
        "shortfall_bps": float(shortfall / notional * 10000) if notional else None,
        "interpretation": "Implementation shortfall measures execution cost versus the intended decision price.",
    }


def slippage_estimate(spread_bps: float, volatility: float, participation_rate: float) -> Dict:
    estimated_bps = 0.5 * spread_bps + 10000 * volatility * np.sqrt(max(participation_rate, 0)) * 0.01
    return {
        "estimated_slippage_bps": float(estimated_bps),
        "inputs": {"spread_bps": spread_bps, "volatility": volatility, "participation_rate": participation_rate},
        "interpretation": "Simple execution-cost model combining spread cost and participation/volatility impact.",
    }
