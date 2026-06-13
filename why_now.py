from datetime import datetime
from typing import Dict, Iterable

import pandas as pd


def evaluate_why_now(ticker: str, analyzed, score_payload: Dict, previous_scores: Iterable[Dict] | None = None, market_payload: Dict | None = None) -> Dict:
    previous_scores = list(previous_scores or [])
    market_payload = market_payload or {}
    latest = analyzed.iloc[-1]
    prior = analyzed.iloc[-2] if len(analyzed) > 1 else latest
    triggers = []

    if _crossed_above(prior, latest, "Close", "SMA50") or _crossed_above(prior, latest, "Close", "SMA200"):
        triggers.append(_trigger("Breakout", "Price reclaimed a major moving average", 75, "Close back below the reclaimed moving average"))

    if latest.get("RS_Breakout"):
        triggers.append(_trigger("Relative strength acceleration", "Relative strength moved from lagging to leading", 80, "Relative strength falls back below its signal line"))

    rel_volume = latest.get("RV")
    if pd.notna(rel_volume) and rel_volume >= 1.8:
        triggers.append(_trigger("Volume spike", f"Relative volume reached {rel_volume:.2f}x", 70, "Volume fades and price cannot hold the move"))

    if latest.get("Close") >= latest.get("High_52W", float("inf")) * 0.995:
        triggers.append(_trigger("52-week high", "Price is pressing against 52-week highs", 65, "Failed breakout below prior range high"))

    if score_payload.get("technical", {}).get("risk_reward", 0) >= 2 and latest.get("Close") > latest.get("SMA50", 0):
        triggers.append(_trigger("Pullback to support", "Risk/reward is favorable near recent support", 55, "Support breaks on heavy volume"))

    final_score = score_payload.get("final_score", 0)
    if previous_scores:
        last_score = previous_scores[0].get("final_score")
        if last_score is not None and final_score - float(last_score) >= 10:
            triggers.append(_trigger("Rank improvement", f"Score improved by {final_score - float(last_score):.1f} points", 85, "Score falls back below prior level"))

    if market_payload.get("risk_environment") == "Risk-off":
        for trigger in triggers:
            trigger["strength"] = max(0, trigger["strength"] - 25)
            trigger["evidence"] += "; market regime is risk-off"

    best = max(triggers, key=lambda item: item["strength"], default=None)
    if not best or best["strength"] < 50:
        return {
            "send_alert": False,
            "reason": "No clear Why Now trigger",
            "evidence": "No recent statistically meaningful change was detected.",
            "date_of_change": datetime.now().date().isoformat(),
            "strength": 0,
            "invalidates": "N/A",
            "triggers": triggers,
        }

    return {
        "send_alert": True,
        "reason": best["reason"],
        "evidence": best["evidence"],
        "date_of_change": datetime.now().date().isoformat(),
        "strength": best["strength"],
        "invalidates": best["invalidates"],
        "triggers": triggers,
    }


def signal_type_from_why_now(payload: Dict) -> str:
    reason = payload.get("reason", "").lower()
    if "breakout" in reason:
        return "breakout"
    if "relative strength" in reason:
        return "relative_strength"
    if "volume" in reason:
        return "volume_spike"
    if "rank" in reason:
        return "rank_improvement"
    if "support" in reason:
        return "pullback_to_support"
    return "watchlist_change"


def _crossed_above(prior, latest, left: str, right: str) -> bool:
    return (
        pd.notna(prior.get(left))
        and pd.notna(prior.get(right))
        and pd.notna(latest.get(left))
        and pd.notna(latest.get(right))
        and prior[left] <= prior[right]
        and latest[left] > latest[right]
    )


def _trigger(reason: str, evidence: str, strength: int, invalidates: str) -> Dict:
    
    return {
        "reason": reason,
        "evidence": evidence,
        "strength": strength,
        "invalidates": invalidates,
    }
