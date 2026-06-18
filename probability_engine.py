"""Lightweight probability engine for stock intelligence.

This is deliberately simple until enough labeled signal outcomes exist for a true
trained model. It converts existing quant features into an interpretable
probability of outperformance, then can later be replaced by logistic regression,
random forest, or XGBoost using the same output contract.
"""

from typing import Dict, Optional

import numpy as np
import pandas as pd


def probability_of_outperformance(
    score: Dict,
    fundamentals: Optional[Dict] = None,
    technical: Optional[Dict] = None,
    catalysts: Optional[Dict] = None,
    market_payload: Optional[Dict] = None,
) -> Dict:
    fundamentals = fundamentals or {}
    technical = technical or {}
    catalysts = catalysts or {}
    market_payload = market_payload or {}
    categories = score.get("categories", {}) if score else {}

    raw_points = 0.0
    evidence = []

    raw_points += _scaled(categories.get("momentum"), 50, 100, 0, 18)
    raw_points += _scaled(categories.get("technical"), 50, 100, 0, 14)
    raw_points += _scaled(categories.get("fundamental"), 50, 100, 0, 14)
    raw_points += _scaled(categories.get("volume"), 40, 100, 0, 10)
    raw_points += _scaled(categories.get("risk"), 40, 100, -10, 10)
    raw_points += _scaled(categories.get("catalyst"), 0, 100, 0, 10)

    if fundamentals.get("revenue_growth") is not None and fundamentals["revenue_growth"] >= 0.20:
        raw_points += 6
        evidence.append("Revenue growth above 20%")
    if fundamentals.get("eps_growth") is not None and fundamentals["eps_growth"] >= 0.15:
        raw_points += 5
        evidence.append("EPS growth above 15%")
    if technical.get("relative_volume") is not None and technical.get("relative_volume", 0) >= 1.5:
        raw_points += 5
        evidence.append("Volume surge above 1.5x")
    if technical.get("breakout"):
        raw_points += 5
        evidence.append("Breakout pressure detected")
    if catalysts.get("earnings_surprise"):
        raw_points += 5
        evidence.append("Positive earnings surprise/catalyst")
    if catalysts.get("analyst_revision"):
        raw_points += 4
        evidence.append("Analyst data is supportive")
    if market_payload.get("risk_environment") == "Risk-off":
        raw_points -= 8
        evidence.append("Risk-off market regime lowers probability")
    if market_payload.get("buy_environment") == "Dangerous":
        raw_points -= 8
        evidence.append("Buy environment is dangerous")

    final_score = score.get("final_score") if score else None
    if final_score is not None:
        evidence.append(f"Composite score {final_score}/100")

    probability = _sigmoid(-0.15 + raw_points / 25)
    probability = max(0.05, min(0.95, probability))
    confidence = _confidence_label(score, fundamentals, technical, catalysts)
    return {
        "model": "rules_to_probability_v1",
        "probability_of_outperformance": round(float(probability), 4),
        "probability_pct": round(float(probability * 100), 2),
        "confidence": confidence,
        "evidence": evidence[:8] if evidence else ["Insufficient feature evidence; using baseline probability."],
        "note": "Heuristic probability until enough labeled outcomes exist for trained ML.",
    }


def feature_snapshot(score: Dict, fundamentals: Optional[Dict] = None, technical: Optional[Dict] = None, catalysts: Optional[Dict] = None) -> Dict:
    fundamentals = fundamentals or {}
    technical = technical or {}
    catalysts = catalysts or {}
    categories = score.get("categories", {}) if score else {}
    return {
        "final_score": score.get("final_score") if score else None,
        "technical_score": categories.get("technical"),
        "momentum_score": categories.get("momentum"),
        "volume_score": categories.get("volume"),
        "fundamental_score": categories.get("fundamental"),
        "risk_score": categories.get("risk"),
        "catalyst_score": categories.get("catalyst"),
        "revenue_growth": fundamentals.get("revenue_growth"),
        "eps_growth": fundamentals.get("eps_growth"),
        "roe": fundamentals.get("roe"),
        "forward_pe": fundamentals.get("forward_pe"),
        "relative_volume": technical.get("relative_volume"),
        "breakout": technical.get("breakout"),
        "price_above_sma50": technical.get("price_above_sma50"),
        "price_above_sma200": technical.get("price_above_sma200"),
        "earnings_surprise": catalysts.get("earnings_surprise"),
        "analyst_revision": catalysts.get("analyst_revision"),
        "major_news": catalysts.get("major_news"),
    }


def _scaled(value, source_low, source_high, target_low, target_high):
    if value is None:
        return 0.0
    try:
        value = float(value)
    except Exception:
        return 0.0
    pct = (value - source_low) / (source_high - source_low)
    pct = max(0, min(1, pct))
    return target_low + pct * (target_high - target_low)


def _sigmoid(value):
    return 1 / (1 + np.exp(-value))


def _confidence_label(score, fundamentals, technical, catalysts):
    filled = 0
    for payload in (score or {}, fundamentals or {}, technical or {}, catalysts or {}):
        if isinstance(payload, dict):
            filled += sum(value is not None for value in payload.values())
    if filled >= 30:
        return "High"
    if filled >= 16:
        return "Moderate"
    return "Low"
