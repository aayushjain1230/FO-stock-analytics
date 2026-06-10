from typing import Dict

import numpy as np
import pandas as pd

import quant_analytics


INDEX_SYMBOLS = {
    "sp500": "SPY",
    "nasdaq": "QQQ",
    "russell2000": "IWM",
    "dow": "DIA",
    "vix": "^VIX",
}

SECTOR_ETFS = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Financials": "XLF",
    "Industrials": "XLI",
    "Energy": "XLE",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Utilities": "XLU",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}


def classify_market(index_data: Dict[str, pd.DataFrame], sector_data: Dict[str, pd.DataFrame] | None = None) -> Dict:
    sector_data = sector_data or {}
    index_metrics = {name: _index_trend(df) for name, df in index_data.items() if df is not None and not df.empty}
    sector_metrics = {name: _sector_trend(df) for name, df in sector_data.items() if df is not None and not df.empty}
    health_score = _health_score(index_metrics, sector_metrics)
    vix_status = _vix_status(index_data.get("vix"))
    risk_environment = _risk_environment(index_metrics, sector_metrics, vix_status)
    regime = _regime_label(index_metrics, health_score, risk_environment)
    return {
        "regime": regime,
        "health_score": health_score,
        "risk_environment": risk_environment,
        "buy_environment": "Favorable" if health_score >= 65 and risk_environment != "Risk-off" else "Selective" if health_score >= 45 else "Dangerous",
        "vix_status": vix_status,
        "index_trends": index_metrics,
        "sector_rankings": sorted(sector_metrics.values(), key=lambda item: item["momentum_score"], reverse=True),
        "sector_participation": _sector_participation(sector_metrics),
        "risk_on_risk_off": _risk_on_score(sector_metrics),
        "breadth_proxy": _breadth_proxy(index_metrics),
    }


def _index_trend(df: pd.DataFrame) -> Dict:
    close = quant_analytics._as_price_series(df)
    latest = close.iloc[-1]
    sma20 = close.rolling(20).mean().iloc[-1]
    sma50 = close.rolling(50).mean().iloc[-1]
    sma200 = close.rolling(200).mean().iloc[-1]
    returns = close.pct_change().dropna()
    above_count = sum(latest > x for x in [sma20, sma50, sma200] if pd.notna(x))
    return {
        "latest": quant_analytics.safe_float(latest),
        "above_sma20": bool(pd.notna(sma20) and latest > sma20),
        "above_sma50": bool(pd.notna(sma50) and latest > sma50),
        "above_sma200": bool(pd.notna(sma200) and latest > sma200),
        "trend_score": round(above_count / 3 * 100, 2),
        "one_month_return": quant_analytics.safe_float(close.pct_change(21).iloc[-1], 0.0),
        "three_month_return": quant_analytics.safe_float(close.pct_change(63).iloc[-1], 0.0),
        "volatility": quant_analytics.safe_float(returns.tail(21).std() * np.sqrt(252), 0.0),
    }


def _sector_trend(df: pd.DataFrame) -> Dict:
    close = quant_analytics._as_price_series(df)
    latest = close.iloc[-1]
    sma50 = close.rolling(50).mean().iloc[-1]
    one_month = close.pct_change(21).iloc[-1] if len(close) > 21 else 0
    three_month = close.pct_change(63).iloc[-1] if len(close) > 63 else 0
    momentum = (max(one_month, -0.25) + max(three_month, -0.35)) * 100 + (25 if latest > sma50 else 0)
    return {
        "name": getattr(df, "name", "Sector"),
        "latest": quant_analytics.safe_float(latest),
        "above_sma50": bool(pd.notna(sma50) and latest > sma50),
        "one_month_return": quant_analytics.safe_float(one_month, 0.0),
        "three_month_return": quant_analytics.safe_float(three_month, 0.0),
        "momentum_score": round(float(max(0, min(100, momentum + 50))), 2),
    }


def _health_score(index_metrics: Dict, sector_metrics: Dict) -> float:
    if not index_metrics:
        return 0.0
    trend = np.mean([payload["trend_score"] for payload in index_metrics.values() if "trend_score" in payload])
    breadth = _breadth_proxy(index_metrics)
    sector_participation = _sector_participation(sector_metrics)
    return round(float(trend * 0.5 + breadth * 0.25 + sector_participation * 0.25), 2)


def _breadth_proxy(index_metrics: Dict) -> float:
    if not index_metrics:
        return 0.0
    positive = sum(1 for payload in index_metrics.values() if payload.get("one_month_return", 0) > 0)
    return round(positive / len(index_metrics) * 100, 2)


def _sector_participation(sector_metrics: Dict) -> float:
    if not sector_metrics:
        return 0.0
    positive = sum(1 for payload in sector_metrics.values() if payload.get("above_sma50"))
    return round(positive / len(sector_metrics) * 100, 2)


def _risk_on_score(sector_metrics: Dict) -> Dict:
    risk_on = ["Technology", "Consumer Discretionary", "Financials", "Industrials"]
    defensive = ["Utilities", "Consumer Staples", "Healthcare"]
    risk_on_avg = np.mean([sector_metrics[name]["momentum_score"] for name in risk_on if name in sector_metrics]) if sector_metrics else 0
    defensive_avg = np.mean([sector_metrics[name]["momentum_score"] for name in defensive if name in sector_metrics]) if sector_metrics else 0
    spread = float(risk_on_avg - defensive_avg)
    return {
        "score": round(spread, 2),
        "label": "Risk-on" if spread > 5 else "Risk-off" if spread < -5 else "Neutral",
    }


def _vix_status(vix_df) -> Dict:
    if vix_df is None or vix_df.empty:
        return {"level": None, "regime": "Unknown"}
    close = quant_analytics._as_price_series(vix_df)
    level = close.iloc[-1]
    if level >= 30:
        regime = "Stress"
    elif level >= 22:
        regime = "Elevated"
    elif level <= 15:
        regime = "Calm"
    else:
        regime = "Normal"
    return {"level": round(float(level), 2), "regime": regime}


def _risk_environment(index_metrics: Dict, sector_metrics: Dict, vix_status: Dict) -> str:
    risk_on = _risk_on_score(sector_metrics)
    sp500 = index_metrics.get("sp500", {})
    if vix_status.get("regime") in ("Stress", "Elevated") and not sp500.get("above_sma50"):
        return "Risk-off"
    if risk_on["label"] == "Risk-on" and sp500.get("above_sma50") and sp500.get("above_sma200"):
        return "Risk-on"
    if not sp500.get("above_sma200"):
        return "High-risk"
    return "Neutral"


def _regime_label(index_metrics: Dict, health_score: float, risk_environment: str) -> str:
    sp500 = index_metrics.get("sp500", {})
    if risk_environment == "Risk-off":
        return "Risk-off"
    if health_score >= 75 and sp500.get("above_sma50") and sp500.get("above_sma200"):
        return "Bull market"
    if health_score <= 30 and not sp500.get("above_sma200"):
        return "Bear market"
    if sp500.get("above_sma200") and not sp500.get("above_sma50"):
        return "Correction"
    if not sp500.get("above_sma200") and sp500.get("one_month_return", 0) > 0:
        return "Recovery"
    if 40 <= health_score <= 60:
        return "Sideways"
    if risk_environment == "High-risk":
        return "High-risk"
    return "Neutral"
