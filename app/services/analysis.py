"""Version 1 explainable watchlist analysis pipeline."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from app.models.evidence import Evidence
from app.models.stock import StockAnalysis, StockSnapshot


def analyze_stock(snapshot: StockSnapshot, history: pd.DataFrame | None = None) -> StockAnalysis:
    """Create a plain-English conclusion from basic evidence only."""
    now = datetime.utcnow()
    positives: list[Evidence] = []
    negatives: list[Evidence] = []
    neutrals: list[Evidence] = []
    unknowns: list[str] = []

    if snapshot.error:
        unknowns.append(snapshot.error)
    _price_evidence(snapshot, positives, negatives, neutrals, now)
    _volume_evidence(snapshot, positives, negatives, neutrals, now)
    if history is not None and not history.empty:
        _moving_average_evidence(history, positives, negatives, neutrals, now)
    else:
        unknowns.append("Not enough price history to evaluate moving-average trend.")
    _fundamental_unknowns(snapshot, unknowns)

    trend = _trend_label(positives, negatives, unknowns)
    volume_status = _volume_status(snapshot)
    confidence = _confidence(positives, negatives, unknowns, snapshot)
    overall_view = _overall_view(trend, volume_status, confidence, positives, negatives)
    what_changed = _what_changed(snapshot, trend, volume_status)
    why_it_matters = _why_it_matters(positives, negatives, volume_status)
    main_risk = _main_risk(snapshot, negatives, unknowns)
    what_to_watch = _what_to_watch(trend, snapshot)
    return StockAnalysis(
        ticker=snapshot.ticker,
        company_name=snapshot.company_name,
        overall_view=overall_view,
        trend=trend,
        volume_status=volume_status,
        confidence=confidence,
        what_changed=what_changed,
        why_it_matters=why_it_matters,
        main_risk=main_risk,
        what_to_watch=what_to_watch,
        positive_evidence=positives,
        negative_evidence=negatives,
        neutral_evidence=neutrals,
        unknowns=unknowns,
        analyzed_at=now,
        snapshot=snapshot,
    )


def _price_evidence(snapshot: StockSnapshot, positives: list[Evidence], negatives: list[Evidence], neutrals: list[Evidence], now: datetime) -> None:
    if snapshot.daily_change_pct is None:
        neutrals.append(_evidence("Daily move unavailable", "price", None, "Daily price movement could not be calculated.", "unknown", "medium", now, "calculation"))
        return
    if snapshot.daily_change_pct >= 0.03:
        positives.append(_evidence("Strong daily gain", "price", snapshot.daily_change_pct, "The stock rose meaningfully today.", "positive", "medium", now, "fact"))
    elif snapshot.daily_change_pct <= -0.03:
        negatives.append(_evidence("Sharp daily decline", "price", snapshot.daily_change_pct, "The stock fell meaningfully today.", "negative", "medium", now, "fact"))
    else:
        neutrals.append(_evidence("Normal daily move", "price", snapshot.daily_change_pct, "The daily move was not large enough to matter by itself.", "neutral", "low", now, "fact"))
    if snapshot.five_day_change_pct is not None:
        if snapshot.five_day_change_pct >= 0.04:
            positives.append(_evidence("Five-day improvement", "price", snapshot.five_day_change_pct, "The stock has been improving over the past week.", "positive", "medium", now, "calculation"))
        elif snapshot.five_day_change_pct <= -0.04:
            negatives.append(_evidence("Five-day weakness", "price", snapshot.five_day_change_pct, "The stock has weakened over the past week.", "negative", "medium", now, "calculation"))


def _volume_evidence(snapshot: StockSnapshot, positives: list[Evidence], negatives: list[Evidence], neutrals: list[Evidence], now: datetime) -> None:
    if snapshot.volume_ratio is None:
        neutrals.append(_evidence("Volume unavailable", "volume", None, "Volume behavior could not be evaluated.", "unknown", "medium", now, "unknown"))
    elif snapshot.volume_ratio >= 1.8:
        target = positives if (snapshot.daily_change_pct or 0) >= 0 else negatives
        target.append(_evidence("Unusual volume", "volume", snapshot.volume_ratio, f"Trading volume was about {snapshot.volume_ratio:.1f}× its normal level.", "positive" if target is positives else "negative", "high", now, "fact"))
    else:
        neutrals.append(_evidence("Normal volume", "volume", snapshot.volume_ratio, "Volume was not unusual.", "neutral", "low", now, "fact"))


def _moving_average_evidence(history: pd.DataFrame, positives: list[Evidence], negatives: list[Evidence], neutrals: list[Evidence], now: datetime) -> None:
    close = pd.to_numeric(history.get("Close"), errors="coerce").dropna()
    if len(close) < 50:
        neutrals.append(_evidence("Short history", "trend", len(close), "There is not enough history for a reliable trend read.", "unknown", "medium", now, "unknown"))
        return
    latest = float(close.iloc[-1])
    sma20 = float(close.tail(20).mean())
    sma50 = float(close.tail(50).mean())
    if latest > sma20 > sma50:
        positives.append(_evidence("Improving trend", "trend", latest, "Price is above recent trend levels, which supports an improving read.", "positive", "high", now, "calculation"))
    elif latest < sma20 < sma50:
        negatives.append(_evidence("Weakening trend", "trend", latest, "Price is below recent trend levels, so the trend remains weak.", "negative", "high", now, "calculation"))
    else:
        neutrals.append(_evidence("Mixed trend", "trend", latest, "Trend evidence is mixed rather than clearly improving or weakening.", "neutral", "medium", now, "calculation"))


def _fundamental_unknowns(snapshot: StockSnapshot, unknowns: list[str]) -> None:
    for label, value in [
        ("Revenue growth", snapshot.revenue_growth),
        ("Earnings growth", snapshot.earnings_growth),
        ("Profit margin", snapshot.profit_margin),
        ("Forward P/E", snapshot.forward_pe),
    ]:
        if value is None:
            unknowns.append(f"{label} is unavailable.")


def _evidence(signal: str, category: str, observed_value: object, interpretation: str, direction: str, importance: str, now: datetime, kind: str) -> Evidence:
    return Evidence(signal, category, observed_value, interpretation, direction, importance, "market_data", now, "fresh", kind)  # type: ignore[arg-type]


def _trend_label(positives: list[Evidence], negatives: list[Evidence], unknowns: list[str]) -> str:
    if any(e.signal == "Improving trend" for e in positives):
        return "Improving"
    if any(e.signal == "Weakening trend" for e in negatives):
        return "Weakening"
    if unknowns and not positives and not negatives:
        return "Unavailable"
    return "Stable"


def _volume_status(snapshot: StockSnapshot) -> str:
    if snapshot.volume_ratio is None:
        return "Unavailable"
    return "Unusual volume" if snapshot.volume_ratio >= 1.8 else "Normal volume"


def _confidence(positives: list[Evidence], negatives: list[Evidence], unknowns: list[str], snapshot: StockSnapshot) -> str:
    if snapshot.price is None:
        return "Insufficient Data"
    independent_categories = {e.category for e in positives + negatives}
    if len(independent_categories) >= 3 and len(unknowns) <= 2:
        return "High"
    if len(independent_categories) >= 2 and len(unknowns) <= 4:
        return "Medium"
    return "Low"


def _overall_view(trend: str, volume_status: str, confidence: str, positives: list[Evidence], negatives: list[Evidence]) -> str:
    if confidence == "Insufficient Data":
        return "Not Enough Evidence"
    if len(negatives) >= 3:
        return "High Risk"
    if trend == "Improving" and len(positives) >= 2:
        return "Positive"
    if trend == "Weakening":
        return "Cautious"
    if positives or negatives:
        return "Worth Watching"
    return "Mixed"


def _what_changed(snapshot: StockSnapshot, trend: str, volume_status: str) -> str:
    if snapshot.price is None:
        return "Price data was unavailable."
    if volume_status == "Unusual volume":
        return "The stock moved with much heavier trading volume than normal."
    if snapshot.daily_change_pct is not None and abs(snapshot.daily_change_pct) >= 0.03:
        direction = "rose" if snapshot.daily_change_pct > 0 else "fell"
        return f"The stock {direction} meaningfully today."
    return f"The stock is currently {trend.lower()} with no major one-day change."


def _why_it_matters(positives: list[Evidence], negatives: list[Evidence], volume_status: str) -> str:
    if volume_status == "Unusual volume":
        return "Higher volume means the move had more participation than an ordinary price change."
    if positives and negatives:
        return "Evidence is mixed, so the stock deserves monitoring rather than a strong conclusion."
    if positives:
        return positives[0].interpretation
    if negatives:
        return negatives[0].interpretation
    return "There is not enough independent evidence for a strong conclusion."


def _main_risk(snapshot: StockSnapshot, negatives: list[Evidence], unknowns: list[str]) -> str:
    if snapshot.next_earnings_date:
        return f"Earnings are approaching or scheduled for {snapshot.next_earnings_date}, so the stock may move sharply."
    if negatives:
        return negatives[0].interpretation
    if unknowns:
        return "Some important data is unavailable, which lowers confidence."
    return "The main risk is that the current evidence fades or reverses."


def _what_to_watch(trend: str, snapshot: StockSnapshot) -> str:
    if trend == "Improving":
        return "Watch whether the stock holds its recent improvement over the next few sessions."
    if trend == "Weakening":
        return "Watch whether the stock can recover above its recent trend area."
    if snapshot.volume_ratio and snapshot.volume_ratio >= 1.8:
        return "Watch whether unusual volume continues or quickly fades."
    return "Watch for a meaningful price move confirmed by stronger-than-normal volume."


def daily_summary(analyses: list[StockAnalysis]) -> str:
    """Generate a deterministic three-to-five sentence daily summary."""
    if not analyses:
        return "No watchlist stocks were analyzed because market data was unavailable."
    attention = sorted(analyses, key=lambda item: item.attention_score(), reverse=True)
    leader = attention[0]
    improving = [a.ticker for a in analyses if a.trend == "Improving"]
    weakening = [a.ticker for a in analyses if a.trend == "Weakening"]
    sentences = [f"{leader.ticker} deserves the most attention because {leader.what_changed.lower()}"]
    if improving:
        sentences.append(f"Improving names: {', '.join(improving[:3])}.")
    if weakening:
        sentences.append(f"Weakening names: {', '.join(weakening[:3])}.")
    sentences.append("The app only reports evidence available from price, volume, basic fundamentals, and known events.")
    return " ".join(sentences)
