"""Cautious large-buyer activity engine."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import numpy as np
import pandas as pd

from app.models.evidence import Evidence
from app.models.sec_filing import FilingRecord
from app.models.stock import StockSnapshot
from app.models.whale import WhaleActivityResult
from app.services.confidence import ConfidenceInputs, confidence_level


def analyze_whale_activity(snapshot: StockSnapshot, history: pd.DataFrame | None, filings: list[FilingRecord] | None = None, intraday: pd.DataFrame | None = None) -> WhaleActivityResult:
    """Estimate whether behavior is consistent with larger buyers or sellers without identifying buyers."""
    filings = filings or []
    now = datetime.now(timezone.utc)
    if history is None or history.empty or "Close" not in history or "Volume" not in history:
        return WhaleActivityResult(snapshot.ticker, "Insufficient Data", None, "Insufficient Data", [], [], [], filings, "There is not enough market data to evaluate Whale Activity.", ["Reliable price and volume history is unavailable."], {}, now)

    frame = history.tail(80).copy()
    close = pd.to_numeric(frame["Close"], errors="coerce")
    volume = pd.to_numeric(frame["Volume"], errors="coerce")
    valid = pd.DataFrame({"close": close, "volume": volume}).dropna()
    if len(valid) < 25:
        return WhaleActivityResult(snapshot.ticker, "Insufficient Data", None, "Insufficient Data", [], [], [], filings, "There is not enough history to separate unusual activity from noise.", ["At least 25 valid observations are required."], {}, now)

    evidence: list[Evidence] = []
    contra: list[Evidence] = []
    confounders = detect_confounders(snapshot, history)
    unknowns: list[str] = []

    volume_score = _volume_anomaly(valid)
    price_score = _price_response(valid)
    multi_score = _multi_day_accumulation(valid)
    closing_score = _closing_pressure(intraday)
    vwap_score = _vwap_behavior(intraday)
    filing_score = _filing_confirmation(filings)
    reliability_score = _signal_reliability(valid, confounders, intraday)

    if volume_score >= 70:
        evidence.append(_evidence("Volume anomaly", "whale_activity", f"{volume_score:.0f}/100", "Trading volume was meaningfully above its recent robust baseline.", "positive", "medium", now, "calculation"))
    if price_score >= 65:
        evidence.append(_evidence("Positive price response", "whale_activity", f"{price_score:.0f}/100", "Abnormal activity occurred with a constructive price response.", "positive", "medium", now, "calculation"))
    elif price_score <= 35:
        contra.append(_evidence("Weak price response", "whale_activity", f"{price_score:.0f}/100", "High activity did not translate into constructive price behavior.", "negative", "medium", now, "calculation"))
    if multi_score >= 65:
        evidence.append(_evidence("Multi-day accumulation pattern", "whale_activity", f"{multi_score:.0f}/100", "Several sessions show above-normal volume on constructive price action.", "positive", "high", now, "calculation"))
    if closing_score is None:
        unknowns.append("Intraday data was unavailable, so late-day pressure was not evaluated.")
    elif closing_score >= 65:
        evidence.append(_evidence("Late-day strength", "whale_activity", f"{closing_score:.0f}/100", "Buying pressure appeared stronger near the close.", "positive", "medium", now, "calculation"))
    if vwap_score is None:
        unknowns.append("VWAP behavior was not evaluated because intraday data was unavailable.")
    elif vwap_score >= 65:
        evidence.append(_evidence("VWAP behavior", "whale_activity", f"{vwap_score:.0f}/100", "Price spent much of the session above estimated VWAP.", "positive", "low", now, "calculation"))
    if filing_score >= 65:
        evidence.append(_evidence("Filing confirmation", "sec_filing", filing_score, "Recent SEC filings provide separate supporting evidence.", "positive", "high", now, "fact"))
    distribution = volume_score >= 65 and price_score <= 35
    if distribution:
        contra.append(_evidence("Distribution risk", "whale_activity", f"{volume_score:.0f}/100 volume", "Heavy volume with weak price behavior can be consistent with sellers overwhelming buyers.", "negative", "high", now, "inference"))

    components = {
        "Volume Anomaly": volume_score,
        "Price Response": price_score,
        "Closing Pressure": closing_score,
        "VWAP Behavior": vwap_score,
        "Multi-Day Accumulation": multi_score,
        "Filing Confirmation": filing_score,
        "Signal Reliability": reliability_score,
    }
    usable_scores = [score for score in components.values() if score is not None]
    raw_score = float(np.mean(usable_scores)) if usable_scores else None
    if raw_score is not None and confounders:
        raw_score = max(0.0, raw_score - 10 * min(len(confounders), 3))

    if distribution:
        level = "Distribution Risk"
    elif raw_score is None or reliability_score < 30:
        level = "Insufficient Data"
    elif raw_score >= 72 and len(evidence) >= 3 and not confounders:
        level = "High"
    elif raw_score >= 55 and len(evidence) >= 2:
        level = "Elevated"
    else:
        level = "Normal"

    confidence = confidence_level(
        ConfidenceInputs(
            price_data_complete=True,
            intraday_data_complete=intraday is not None and not intraday.empty,
            filing_data_fresh=bool(filings),
            independent_evidence_categories=len({item.category for item in evidence}),
            contradicting_evidence_count=len(contra),
            model_agreement="Medium" if raw_score and raw_score > 55 else "Unknown",
            calibration_sample_size=0,
            upcoming_earnings=any("earnings" in item.lower() for item in confounders),
            confounder_count=len(confounders),
        )
    )
    inference = _inference(level, bool(filings), confounders)
    return WhaleActivityResult(snapshot.ticker, level, raw_score, confidence, evidence, contra, confounders, filings, inference, unknowns, components, now)


def detect_confounders(snapshot: StockSnapshot, history: pd.DataFrame | None = None) -> list[str]:
    """Detect common reasons abnormal activity may not indicate quiet accumulation."""
    confounders: list[str] = []
    if snapshot.next_earnings_date:
        try:
            days = (datetime.fromisoformat(str(snapshot.next_earnings_date)[:10]).date() - datetime.utcnow().date()).days
            if -2 <= days <= 10:
                confounders.append(f"Earnings are near ({snapshot.next_earnings_date}).")
        except Exception:
            confounders.append("Earnings date is present but could not be parsed.")
    if history is not None and not history.empty and "Close" in history:
        returns = pd.to_numeric(history["Close"], errors="coerce").pct_change().dropna()
        if len(returns) >= 20 and abs(float(returns.iloc[-1])) > max(0.07, float(returns.tail(20).std() * 3)):
            confounders.append("A very large price move may reflect news or event trading.")
    return confounders


def _volume_anomaly(valid: pd.DataFrame) -> float:
    latest = float(valid["volume"].iloc[-1])
    baseline = float(valid["volume"].tail(60).median())
    if baseline <= 0:
        return 0.0
    ratio = latest / baseline
    mad = float((valid["volume"].tail(60) - baseline).abs().median()) or baseline
    robust_z = (latest - baseline) / (1.4826 * mad)
    return float(np.clip(35 + ratio * 20 + robust_z * 10, 0, 100))


def _price_response(valid: pd.DataFrame) -> float:
    ret = float(valid["close"].iloc[-1] / valid["close"].iloc[-2] - 1)
    close_range = float(valid["close"].tail(20).pct_change().std() or 0.02)
    return float(np.clip(50 + (ret / max(close_range, 0.005)) * 18, 0, 100))


def _multi_day_accumulation(valid: pd.DataFrame) -> float:
    recent = valid.tail(10).copy()
    recent["ret"] = recent["close"].pct_change()
    vol_median = float(valid["volume"].tail(60).median())
    positive_heavy = ((recent["ret"] > 0) & (recent["volume"] > vol_median * 1.2)).sum()
    negative_heavy = ((recent["ret"] < 0) & (recent["volume"] > vol_median * 1.2)).sum()
    obv_like = float(np.sign(recent["ret"].fillna(0)).mul(recent["volume"]).sum())
    obv_score = 15 if obv_like > 0 else -10
    return float(np.clip(45 + positive_heavy * 10 - negative_heavy * 12 + obv_score, 0, 100))


def _closing_pressure(intraday: pd.DataFrame | None) -> float | None:
    if intraday is None or intraday.empty or "Close" not in intraday or "Volume" not in intraday or len(intraday) < 6:
        return None
    tail = intraday.tail(6)
    day = intraday
    final_return = float(tail["Close"].iloc[-1] / tail["Close"].iloc[0] - 1)
    final_volume_share = float(tail["Volume"].sum() / max(day["Volume"].sum(), 1))
    close_location = float((day["Close"].iloc[-1] - day["Low"].min()) / max(day["High"].max() - day["Low"].min(), 0.01)) if "High" in day and "Low" in day else 0.5
    return float(np.clip(45 + final_return * 1000 + final_volume_share * 80 + close_location * 20, 0, 100))


def _vwap_behavior(intraday: pd.DataFrame | None) -> float | None:
    if intraday is None or intraday.empty or "Close" not in intraday or "Volume" not in intraday:
        return None
    volume = pd.to_numeric(intraday["Volume"], errors="coerce").fillna(0)
    close = pd.to_numeric(intraday["Close"], errors="coerce").fillna(method="ffill")
    if volume.sum() <= 0:
        return None
    vwap = float((close * volume).sum() / volume.sum())
    pct_above = float((close > vwap).mean())
    close_above = 1 if close.iloc[-1] > vwap else 0
    return float(np.clip(35 + pct_above * 40 + close_above * 25, 0, 100))


def _filing_confirmation(filings: Iterable[FilingRecord]) -> float:
    score = 0.0
    for filing in filings:
        tx = filing.transaction_type or ""
        if filing.form_type.upper().startswith("SC 13D"):
            score = max(score, 85)
        elif filing.form_type.upper().startswith("SC 13G"):
            score = max(score, 72)
        elif tx == "Open-market purchase":
            score = max(score, 78)
        elif tx.startswith("Delayed 13F"):
            score = max(score, 55)
    return score


def _signal_reliability(valid: pd.DataFrame, confounders: list[str], intraday: pd.DataFrame | None) -> float:
    score = 55 + min(len(valid), 80) * 0.3
    if intraday is None or intraday.empty:
        score -= 10
    score -= len(confounders) * 15
    return float(np.clip(score, 0, 100))


def _evidence(signal: str, category: str, observed_value: object, interpretation: str, direction: str, importance: str, now: datetime, kind: str) -> Evidence:
    return Evidence(signal, category, observed_value, interpretation, direction, importance, "market_data", now, "fresh", kind)  # type: ignore[arg-type]


def _inference(level: str, has_filing: bool, confounders: list[str]) -> str:
    if level == "High":
        return "Several independent signals are consistent with larger buyers participating, but public trading data cannot identify anonymous buyers."
    if level == "Elevated":
        return "Trading behavior is consistent with larger-buyer participation, but the conclusion is conditional and not proof."
    if level == "Distribution Risk":
        return "Heavy activity with weak price behavior raises the risk that sellers are dominating demand."
    if level == "Insufficient Data":
        return "The engine abstained because the data is not reliable enough."
    if has_filing:
        return "SEC filings provide context, but market behavior is not strong enough for an elevated Whale Activity conclusion."
    if confounders:
        return "Confounders make the activity harder to interpret."
    return "Activity looks normal relative to recent history."
