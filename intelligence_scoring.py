from typing import Dict, Optional

import numpy as np
import pandas as pd

import quant_analytics


def _clip(value, low=0, high=100):
    try:
        if pd.isna(value):
            return 0
        return max(low, min(high, float(value)))
    except Exception:
        return 0


def _latest(df: pd.DataFrame):
    
    return df.iloc[-1] if df is not None and not df.empty else pd.Series(dtype=float)


def technical_score(df: pd.DataFrame) -> Dict:
    latest = _latest(df)
    close = latest.get("Close")
    score = 0
    evidence = []

    for label, column, points in [("SMA20", "SMA20", 8), ("SMA50", "SMA50", 10), ("SMA200", "SMA200", 12)]:
        if pd.notna(close) and pd.notna(latest.get(column)) and close > latest[column]:
            score += points
            evidence.append(f"Price above {label}")

    if latest.get("SMA20", 0) > latest.get("SMA50", 0) > latest.get("SMA200", 0):
        score += 15
        evidence.append("SMA alignment is constructive")

    if latest.get("EMA20", 0) > latest.get("EMA50", 0):
        score += 10
        evidence.append("EMA trend is positive")

    rsi = latest.get("RSI")
    if pd.notna(rsi):
        if 50 <= rsi <= 70:
            score += 15
            evidence.append("RSI confirms momentum without extreme extension")
        elif 40 <= rsi < 50 or 70 < rsi <= 80:
            score += 8

    macd = latest.get("MACD")
    macd_signal = latest.get("MACD_Signal")
    if pd.notna(macd) and pd.notna(macd_signal) and macd > macd_signal:
        score += 10
        evidence.append("MACD is above signal")

    dist_sma20 = latest.get("Dist_SMA20")
    if pd.notna(dist_sma20):
        if dist_sma20 <= 3:
            score += 10
            evidence.append("Stock is not materially extended from SMA20")
        else:
            evidence.append("Stock is extended from SMA20")

    support_resistance = support_resistance_levels(df)
    if support_resistance.get("risk_reward", 0) >= 2:
        score += 10
        evidence.append("Support/resistance profile offers positive risk/reward")

    return {"score": round(_clip(score), 2), "evidence": evidence, **support_resistance}


def momentum_score(df: pd.DataFrame, benchmark_df: Optional[pd.DataFrame] = None, sector_df: Optional[pd.DataFrame] = None) -> Dict:
    close = quant_analytics._as_price_series(df)
    score = 0
    evidence = []
    returns = {}
    for label, days, points in [("1m", 21, 10), ("3m", 63, 15), ("6m", 126, 15), ("12m", 252, 20)]:
        value = close.pct_change(days).iloc[-1] if len(close) > days else np.nan
        returns[label] = quant_analytics.safe_float(value, 0.0)
        if pd.notna(value) and value > 0:
            score += points
            evidence.append(f"{label} return is positive")

    acceleration = returns.get("3m", 0) - returns.get("6m", 0) / 2
    if acceleration > 0:
        score += 10
        evidence.append("Momentum is accelerating")

    if benchmark_df is not None:
        bench_close = quant_analytics._as_price_series(benchmark_df)
        if len(close) > 63 and len(bench_close) > 63:
            stock_3m = close.pct_change(63).iloc[-1]
            bench_3m = bench_close.pct_change(63).iloc[-1]
            if pd.notna(stock_3m) and pd.notna(bench_3m) and stock_3m > bench_3m:
                score += 15
                evidence.append("Outperforming S&P 500 over 3 months")

    if sector_df is not None:
        sector_close = quant_analytics._as_price_series(sector_df)
        if len(close) > 63 and len(sector_close) > 63:
            if close.pct_change(63).iloc[-1] > sector_close.pct_change(63).iloc[-1]:
                score += 15
                evidence.append("Outperforming sector ETF over 3 months")

    return {"score": round(_clip(score), 2), "returns": returns, "acceleration": round(float(acceleration), 4), "evidence": evidence}


def volume_score(df: pd.DataFrame) -> Dict:
    if "Volume" not in df.columns:
        return {"score": 0, "evidence": ["Volume data unavailable"]}
    latest = _latest(df)
    volume = pd.to_numeric(df["Volume"], errors="coerce")
    close = pd.to_numeric(df["Close"], errors="coerce")
    avg_volume = volume.rolling(20).mean().iloc[-1]
    relative_volume = latest.get("Volume") / avg_volume if avg_volume else np.nan
    score = 0
    evidence = []
    if pd.notna(relative_volume) and relative_volume >= 1.5:
        score += 25
        evidence.append(f"Relative volume is {relative_volume:.2f}x")
    if len(df) > 1 and latest.get("Close") > df["Close"].iloc[-2] and pd.notna(relative_volume) and relative_volume >= 1.25:
        score += 25
        evidence.append("Price advanced on above-average volume")
    accumulation = ((close > close.shift(1)) & (volume > volume.rolling(20).mean())).tail(20).sum()
    distribution = ((close < close.shift(1)) & (volume > volume.rolling(20).mean())).tail(20).sum()
    if accumulation > distribution:
        score += 25
        evidence.append("Accumulation days exceed distribution days")
    if latest.get("Close", 0) >= close.rolling(63).max().iloc[-1] * 0.98 and pd.notna(relative_volume) and relative_volume >= 1.5:
        score += 25
        evidence.append("High-volume breakout behavior")
    return {
        "score": round(_clip(score), 2),
        "average_volume": quant_analytics.safe_float(avg_volume, 0.0),
        "relative_volume": quant_analytics.safe_float(relative_volume, 0.0),
        "accumulation_days": int(accumulation),
        "distribution_days": int(distribution),
        "evidence": evidence,
    }


def risk_score(df: pd.DataFrame, benchmark_df: Optional[pd.DataFrame] = None) -> Dict:
    returns = quant_analytics.daily_returns(df)
    benchmark_returns = quant_analytics.daily_returns(benchmark_df) if benchmark_df is not None else pd.Series(dtype=float)
    risk = quant_analytics.comprehensive_stock_analysis(df, benchmark_df).get("risk", {})
    score = 100
    evidence = []
    if risk.get("annualized_volatility", 0) > 0.55:
        score -= 25
        evidence.append("Volatility is elevated")
    if risk.get("maximum_drawdown", 0) < -0.35:
        score -= 25
        evidence.append("Historical drawdown is severe")
    if abs(risk.get("beta", 0)) > 1.5:
        score -= 15
        evidence.append("Beta indicates elevated market sensitivity")
    if returns.tail(21).std() > returns.std() * 1.25:
        score -= 15
        evidence.append("Short-term volatility is rising")
    if benchmark_returns.empty:
        evidence.append("Benchmark-relative risk unavailable")
    return {"score": round(_clip(score), 2), "risk_metrics": risk, "evidence": evidence}


def fundamental_score(fundamentals: Optional[Dict] = None) -> Dict:
    fundamentals = fundamentals or {}
    score = 50
    evidence = []
    for key in ("revenue_growth", "eps_growth", "fcf_growth"):
        value = fundamentals.get(key)
        if value is not None and value > 0.10:
            score += 8
            evidence.append(f"{key.replace('_', ' ')} is strong")
        elif value is not None and value < 0:
            score -= 8
    for key in ("gross_margin", "operating_margin", "net_margin", "roe", "roic"):
        value = fundamentals.get(key)
        if value is not None and value > 0.15:
            score += 5
    if fundamentals.get("debt_to_equity") is not None and fundamentals["debt_to_equity"] > 2:
        score -= 10
        evidence.append("Debt-to-equity is elevated")
    if fundamentals.get("forward_pe") is not None and fundamentals.get("eps_growth") is not None:
        if fundamentals["forward_pe"] > 40 and fundamentals["eps_growth"] < 0.15:
            score -= 10
            evidence.append("Valuation appears demanding versus growth")
    classification = classify_fundamentals(score, fundamentals)
    return {"score": round(_clip(score), 2), "classification": classification, "evidence": evidence}


def catalyst_score(catalysts: Optional[Dict] = None) -> Dict:
    catalysts = catalysts or {}
    score = 0
    evidence = []
    for key, points in [
        ("earnings_surprise", 20),
        ("analyst_revision", 20),
        ("insider_buying", 15),
        ("sector_strength", 20),
        ("major_news", 15),
        ("guidance_change", 10),
    ]:
        if catalysts.get(key):
            score += points
            evidence.append(key.replace("_", " "))
    return {"score": round(_clip(score), 2), "evidence": evidence}


def final_stock_score(df: pd.DataFrame, benchmark_df: Optional[pd.DataFrame] = None, sector_df: Optional[pd.DataFrame] = None, fundamentals: Optional[Dict] = None, catalysts: Optional[Dict] = None) -> Dict:
    technical = technical_score(df)
    momentum = momentum_score(df, benchmark_df, sector_df)
    volume = volume_score(df)
    fundamental = fundamental_score(fundamentals)
    risk = risk_score(df, benchmark_df)
    catalyst = catalyst_score(catalysts)

    categories = {
        "technical": technical["score"],
        "momentum": momentum["score"],
        "volume": volume["score"],
        "fundamental": fundamental["score"],
        "risk": risk["score"],
        "catalyst": catalyst["score"],
    }
    final = (
        categories["technical"] * 0.25
        + categories["momentum"] * 0.25
        + categories["volume"] * 0.15
        + categories["fundamental"] * 0.15
        + categories["risk"] * 0.15
        + categories["catalyst"] * 0.05
    )
    confidence = confidence_score(categories)
    rating = rating_label(final, risk["score"])
    risk_level = "High" if risk["score"] < 45 else "Moderate" if risk["score"] < 70 else "Controlled"
    evidence = technical["evidence"] + momentum["evidence"] + volume["evidence"] + fundamental["evidence"] + catalyst["evidence"]
    return {
        "final_score": round(float(final), 2),
        "rating": rating,
        "confidence": confidence,
        "risk_level": risk_level,
        "categories": categories,
        "technical": technical,
        "momentum": momentum,
        "volume": volume,
        "fundamental": fundamental,
        "risk": risk,
        "catalyst": catalyst,
        "explanation": "; ".join(evidence[:6]) if evidence else "No high-conviction evidence yet.",
    }


def support_resistance_levels(df: pd.DataFrame) -> Dict:
    close = pd.to_numeric(df["Close"], errors="coerce")
    latest = close.iloc[-1]
    support = close.tail(63).min()
    resistance = close.tail(63).max()
    downside = latest - support
    upside = resistance - latest
    risk_reward = upside / downside if downside and downside > 0 else 0
    return {
        "support": quant_analytics.safe_float(support),
        "resistance": quant_analytics.safe_float(resistance),
        "risk_reward": round(float(risk_reward), 2) if np.isfinite(risk_reward) else 0,
    }


def classify_fundamentals(score: float, fundamentals: Dict) -> str:
    if score >= 75 and fundamentals.get("revenue_growth", 0) > 0.15:
        return "Quality Compounder"
    if score >= 70:
        return "Fundamentally Strong"
    if score >= 50:
        return "Fundamentally Average"
    if fundamentals.get("forward_pe", 0) and fundamentals.get("forward_pe") < 12 and score < 45:
        return "Cheap but Weak"
    if fundamentals.get("revenue_growth", 0) > 0.25 and fundamentals.get("forward_pe", 0) > 50:
        return "High Growth but Expensive"
    if score < 35:
        return "Fundamentally Deteriorating"
    return "Fundamentally Weak"


def rating_label(score: float, risk_score_value: float) -> str:
    if risk_score_value < 35:
        return "Avoid"
    if score >= 80:
        return "Buy Watch"
    if score >= 65:
        return "Watch"
    if score >= 45:
        return "Hold"
    if score >= 30:
        return "Drop"
    return "Avoid"


def confidence_score(categories: Dict[str, float]) -> float:
    values = np.array(list(categories.values()), dtype=float)
    mean = values.mean()
    dispersion_penalty = values.std() * 0.25
    data_penalty = (values == 0).sum() * 5
    return round(_clip(mean - dispersion_penalty - data_penalty), 2)
