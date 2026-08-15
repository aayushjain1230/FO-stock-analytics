"""
fundamentals_fetcher.py
=======================
Fetches, normalizes, and caches fundamental, earnings, and catalyst data from yfinance.

This module intentionally contains data-fetching logic only. Position sizing belongs in
portfolio/risk modules, not in the fundamentals fetcher.
"""

import json
import os
import time
from typing import Dict, Optional

import numpy as np
import pandas as pd

import intelligence_scoring

CACHE_DIR = os.path.join("cache", "fundamentals")
CACHE_TTL_SECONDS = 3600 * 6

ETF_SYMBOLS = {
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "VGT",
    "XLK", "XLF", "XLV", "XLY", "XLP", "XLE", "XLI",
    "XLB", "XLU", "XLRE", "XLC", "SMH", "SOXX", "TLT",
    "IWD", "MTUM", "SPLV",
}


def is_etf_symbol(ticker: str) -> bool:
    return str(ticker or "").upper() in ETF_SYMBOLS


def _etf_payload(ticker: str) -> Dict:
    return {
        "ticker": ticker.upper(),
        "available": False,
        "asset_type": "ETF",
        "classification": "ETF / Fund",
        "note": "Company fundamentals skipped because this symbol is an ETF/fund.",
    }


def _cache_path(ticker: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{ticker.upper()}.json")


def _cache_read(ticker: str) -> Optional[Dict]:
    path = _cache_path(ticker)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        if "data" in payload and "cached_at" in payload:
            cached_at = payload.get("cached_at", 0)
            data = payload.get("data", {})
        else:
            cached_at = payload.get("_cached_at", 0)
            data = {k: v for k, v in payload.items() if k != "_cached_at"}

        if time.time() - cached_at < CACHE_TTL_SECONDS:
            data["from_cache"] = True
            return data
    except Exception:
        return None
    return None


def _cache_write(ticker: str, data: Dict) -> None:
    try:
        with open(_cache_path(ticker), "w", encoding="utf-8") as f:
            json.dump({"cached_at": time.time(), "data": data}, f, default=str)
    except Exception:
        pass


def _safe(value, default=None):
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)) or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _safe_str(value) -> Optional[str]:
    try:
        if value is None:
            return None
        return str(value)
    except Exception:
        return None


def _earnings_date(info: Dict) -> Optional[str]:
    value = info.get("earningsDate") or info.get("earningsTimestamp")
    if isinstance(value, list) and value:
        return str(value[0])
    return str(value) if value else None


def fetch_fundamentals(yf_module, ticker: str) -> Dict:
    if is_etf_symbol(ticker):
        return _etf_payload(ticker)

    payload: Dict = {"ticker": ticker.upper(), "available": False}
    try:
        stock = yf_module.Ticker(ticker)
        info = stock.info or {}
    except Exception as exc:
        payload["error"] = str(exc)
        return payload

    if not info:
        return payload

    raw_dte = _safe(info.get("debtToEquity"))
    debt_to_equity = raw_dte / 100 if raw_dte is not None and raw_dte > 10 else raw_dte
    fcf = _safe(info.get("freeCashflow"))
    market_cap = _safe(info.get("marketCap"))
    fcf_yield = fcf / market_cap if fcf and market_cap and market_cap > 0 else None

    payload.update({
        "available": True,
        "company_name": info.get("longName") or info.get("shortName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "exchange": info.get("exchange"),
        "market_cap": market_cap,
        "revenue_growth": _safe(info.get("revenueGrowth")),
        "eps_growth": _safe(info.get("earningsGrowth")),
        "fcf_growth": None,
        "ebitda_growth": None,
        "gross_margin": _safe(info.get("grossMargins")),
        "operating_margin": _safe(info.get("operatingMargins")),
        "net_margin": _safe(info.get("profitMargins")),
        "roe": _safe(info.get("returnOnEquity")),
        "roa": _safe(info.get("returnOnAssets")),
        "roic": None,
        "debt_to_equity": debt_to_equity,
        "current_ratio": _safe(info.get("currentRatio")),
        "quick_ratio": _safe(info.get("quickRatio")),
        "cash_position": _safe(info.get("totalCash")),
        "total_debt": _safe(info.get("totalDebt")),
        "interest_coverage": None,
        "forward_pe": _safe(info.get("forwardPE")),
        "trailing_pe": _safe(info.get("trailingPE")),
        "peg": _safe(info.get("pegRatio")),
        "price_to_book": _safe(info.get("priceToBook")),
        "price_to_sales": _safe(info.get("priceToSalesTrailing12Months")),
        "enterprise_to_ebitda": _safe(info.get("enterpriseToEbitda")),
        "enterprise_to_revenue": _safe(info.get("enterpriseToRevenue")),
        "fcf_yield": fcf_yield,
        "institutional_ownership": _safe(info.get("heldPercentInstitutions")),
        "insider_ownership": _safe(info.get("heldPercentInsiders")),
        "short_interest": _safe(info.get("sharesShort")),
        "short_ratio": _safe(info.get("shortRatio")),
        "short_percent_float": _safe(info.get("shortPercentOfFloat")),
        "days_to_cover": _safe(info.get("shortRatio")),
        "target_mean_price": _safe(info.get("targetMeanPrice")),
        "target_high_price": _safe(info.get("targetHighPrice")),
        "target_low_price": _safe(info.get("targetLowPrice")),
        "recommendation": _safe_str(info.get("recommendationKey")),
        "number_of_analysts": _safe(info.get("numberOfAnalystOpinions")),
        "dividend_yield": _safe(info.get("dividendYield")),
        "payout_ratio": _safe(info.get("payoutRatio")),
        "next_earnings_date": _earnings_date(info),
        "eps_current_year": _safe(info.get("trailingEps")),
        "eps_forward": _safe(info.get("forwardEps")),
    })
    fundamental = intelligence_scoring.fundamental_score(payload)
    payload["classification"] = fundamental.get("classification")
    return payload


def fetch_fundamentals_cached(yf_module, ticker: str, use_cache: bool = True) -> Dict:
    if is_etf_symbol(ticker):
        return _etf_payload(ticker)

    cached = _cache_read(ticker) if use_cache else None
    if cached is not None:
        return cached
    data = fetch_fundamentals(yf_module, ticker)
    if data.get("available"):
        _cache_write(ticker, data)
    return data


def fetch_earnings_history(yf_module, ticker: str) -> Dict:
    if is_etf_symbol(ticker):
        return {
            "available": False,
            "history": [],
            "beat_streak": 0,
            "miss_streak": 0,
            "average_surprise_pct": None,
            "last_surprise_pct": None,
            "last_surprise_direction": None,
            "note": "Earnings history skipped because this symbol is an ETF/fund.",
        }

    result = {
        "available": False,
        "history": [],
        "beat_streak": 0,
        "miss_streak": 0,
        "average_surprise_pct": None,
        "last_surprise_pct": None,
        "last_surprise_direction": None,
    }
    try:
        stock = yf_module.Ticker(ticker)
        earnings_history = getattr(stock, "earnings_history", None)
        if earnings_history is not None and not earnings_history.empty:
            rows = []
            for idx, row in earnings_history.iterrows():
                eps_est = _safe(row.get("epsEstimate"))
                eps_act = _safe(row.get("epsActual"))
                surprise = _safe(row.get("surprisePercent"))
                rows.append({
                    "date": str(idx.date()) if hasattr(idx, "date") else str(idx),
                    "eps_estimate": eps_est,
                    "eps_actual": eps_act,
                    "surprise_pct": surprise,
                    "beat": (surprise or 0) > 0,
                })
            rows = sorted(rows, key=lambda item: item["date"], reverse=True)
            result["history"] = rows[:8]
            result["available"] = bool(rows)

        if not result["available"]:
            quarterly_earnings = getattr(stock, "quarterly_earnings", None)
            if quarterly_earnings is not None and not quarterly_earnings.empty:
                rows = []
                for idx, row in quarterly_earnings.iterrows():
                    est = _safe(row.get("EPS Estimate"))
                    act = _safe(row.get("Reported EPS"))
                    surprise = ((act - est) / abs(est) * 100) if est and act and est != 0 else None
                    rows.append({
                        "date": str(idx),
                        "eps_estimate": est,
                        "eps_actual": act,
                        "surprise_pct": surprise,
                        "beat": (surprise or 0) > 0,
                    })
                rows = sorted(rows, key=lambda item: item["date"], reverse=True)
                result["history"] = rows[:8]
                result["available"] = bool(rows)

        if result["history"]:
            surprises = [row["surprise_pct"] for row in result["history"] if row.get("surprise_pct") is not None]
            if surprises:
                result["average_surprise_pct"] = round(float(np.mean(surprises)), 2)
                result["last_surprise_pct"] = surprises[0]
                result["last_surprise_direction"] = "beat" if surprises[0] > 0 else "miss"

            beat_streak = 0
            for row in result["history"]:
                if row.get("beat"):
                    beat_streak += 1
                else:
                    break
            miss_streak = 0
            for row in result["history"]:
                if not row.get("beat"):
                    miss_streak += 1
                else:
                    break
            result["beat_streak"] = beat_streak
            result["miss_streak"] = miss_streak
    except Exception as exc:
        result["error"] = str(exc)
    return result


def fetch_catalysts(yf_module, ticker: str, fundamentals: Optional[Dict] = None, news: Optional[Dict] = None, technical: Optional[Dict] = None) -> Dict:
    fundamentals = fundamentals or {}
    if is_etf_symbol(ticker):
        technical = technical or {}
        news_items = (news or {}).get("items", [])
        return {
            "earnings_surprise": False,
            "analyst_revision": False,
            "insider_buying": False,
            "sector_strength": bool(technical.get("price_above_sma50") and technical.get("price_above_sma200")),
            "major_news": bool(news_items and max(item.get("importance_score", 0) for item in news_items) >= 60),
            "guidance_change": False,
            "bullish_news_score": sum(item.get("bullish_score", 0) for item in news_items),
            "bearish_news_score": sum(item.get("bearish_score", 0) for item in news_items),
            "institutional_ownership": None,
            "detail": {"note": "Company catalysts skipped because this symbol is an ETF/fund."},
            "evidence": ["ETF/fund evaluated on technical, trend, volume, and portfolio context"],
        }

    news_items = (news or {}).get("items", [])
    technical = technical or {}
    catalysts: Dict = {
        "earnings_surprise": False,
        "analyst_revision": False,
        "insider_buying": False,
        "sector_strength": bool(technical.get("price_above_sma50") and technical.get("price_above_sma200")),
        "major_news": bool(news_items and max(item.get("importance_score", 0) for item in news_items) >= 60),
        "guidance_change": False,
        "bullish_news_score": sum(item.get("bullish_score", 0) for item in news_items),
        "bearish_news_score": sum(item.get("bearish_score", 0) for item in news_items),
        "institutional_ownership": fundamentals.get("institutional_ownership"),
        "detail": {},
        "evidence": [],
    }

    try:
        stock = yf_module.Ticker(ticker)

        earnings = fetch_earnings_history(yf_module, ticker)
        last_surprise = earnings.get("last_surprise_pct")
        if last_surprise is not None and last_surprise > 2:
            catalysts["earnings_surprise"] = True
        elif fundamentals.get("eps_growth") and fundamentals.get("eps_growth") > 0.10:
            catalysts["earnings_surprise"] = True
        catalysts["detail"]["earnings"] = earnings

        current_price = None
        try:
            hist = stock.history(period="5d")
            if hist is not None and not hist.empty:
                current_price = float(hist["Close"].dropna().iloc[-1])
        except Exception:
            current_price = None

        target_mean = fundamentals.get("target_mean_price")
        recommendation = str(fundamentals.get("recommendation") or "").lower()
        if target_mean and current_price and current_price > 0:
            upside = (target_mean - current_price) / current_price
            catalysts["detail"]["analyst_upside_pct"] = round(upside * 100, 2)
            if upside > 0.10 and recommendation in {"buy", "strong_buy"}:
                catalysts["analyst_revision"] = True
        elif recommendation in {"buy", "strong_buy"}:
            catalysts["analyst_revision"] = True

        try:
            insider_df = getattr(stock, "insider_purchases", None)
            if insider_df is not None and not insider_df.empty:
                transaction_col = next((col for col in insider_df.columns if "transaction" in col.lower()), None)
                if transaction_col:
                    buys = insider_df[transaction_col].astype(str).str.contains("purchase|buy", case=False, na=False).sum()
                    catalysts["insider_buying"] = bool(buys)
                    catalysts["detail"]["insider_purchases_count"] = int(buys)
        except Exception:
            pass

        if not catalysts["insider_buying"] and fundamentals.get("insider_ownership") and fundamentals.get("insider_ownership") >= 0.05:
            catalysts["insider_buying"] = True

        news_delta = catalysts["bullish_news_score"] - catalysts["bearish_news_score"]
        if abs(news_delta) >= 2:
            catalysts["guidance_change"] = True

        eps_current = fundamentals.get("eps_current_year")
        eps_forward = fundamentals.get("eps_forward")
        if eps_current and eps_forward and eps_current != 0:
            eps_growth = (eps_forward - eps_current) / abs(eps_current)
            catalysts["detail"]["eps_growth_implied"] = round(eps_growth * 100, 2)
            if eps_growth > 0.10:
                catalysts["guidance_change"] = True

    except Exception as exc:
        catalysts["error"] = str(exc)

    catalysts["evidence"] = _catalyst_evidence(catalysts)
    return catalysts


def earnings_snapshot(yf_module, ticker: str, fundamentals: Optional[Dict] = None) -> Dict:
    fundamentals = fundamentals or {}
    earnings = fetch_earnings_history(yf_module, ticker)
    if earnings.get("available"):
        surprise = earnings.get("last_surprise_pct")
        return {
            "next_earnings_date": fundamentals.get("next_earnings_date"),
            "surprise_tracking": "Available",
            "beat_streak": earnings.get("beat_streak"),
            "miss_streak": earnings.get("miss_streak"),
            "average_surprise_pct": earnings.get("average_surprise_pct"),
            "last_surprise_pct": surprise,
            "last_surprise_direction": earnings.get("last_surprise_direction"),
            "risk_score": 65 if fundamentals.get("next_earnings_date") else 35,
            "history": earnings.get("history", []),
        }
    return {
        "next_earnings_date": fundamentals.get("next_earnings_date"),
        "surprise_tracking": "Unavailable from current data provider.",
        "risk_score": 50 if fundamentals.get("next_earnings_date") else 25,
        "note": earnings.get("error") or "Earnings surprise data unavailable from yfinance.",
    }


def _catalyst_evidence(catalysts: Dict) -> list:
    labels = {
        "earnings_surprise": "recent earnings surprise or earnings growth is positive",
        "analyst_revision": "analyst target/recommendation data is supportive",
        "insider_buying": "insider ownership or purchase data is supportive",
        "sector_strength": "technical backdrop is constructive",
        "major_news": "recent news item has elevated importance",
        "guidance_change": "news tone or forward EPS implies a catalyst shift",
    }
    return [label for key, label in labels.items() if catalysts.get(key)]
