import json
import os
import time
from datetime import datetime
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd

import fundamentals_fetcher
import intelligence_scoring
import portfolio_engine
import probability_engine
import quant_analytics
import why_now


SCREENERS_FILE = os.path.join("config", "screeners.json")
FUNDAMENTALS_CACHE_DIR = os.path.join("cache", "fundamentals")
FUNDAMENTALS_TTL_SECONDS = 6 * 60 * 60


DEFAULT_SCREENERS = {
    "quality_momentum": {
        "min_final_score": 60,
        "min_revenue_growth": 0.05,
        "min_gross_margin": 0.25,
        "max_forward_pe": 80,
        "min_rsi": 45,
        "max_rsi": 78,
        "require_price_above_sma200": True,
    },
    "technical_breakout": {
        "min_final_score": 55,
        "min_relative_volume": 1.2,
        "require_52w_pressure": True,
        "require_price_above_sma50": True,
    },
    "value_watch": {
        "max_forward_pe": 25,
        "max_price_to_book": 5,
        "min_final_score": 45,
    },
}


def load_screeners(path: str = SCREENERS_FILE) -> Dict:
    if not os.path.exists(path):
        return DEFAULT_SCREENERS
    try:
        with open(path, "r") as f:
            payload = json.load(f)
        return {**DEFAULT_SCREENERS, **payload}
    except Exception:
        return DEFAULT_SCREENERS


def fetch_fundamentals(yf_module, ticker: str, use_cache: bool = True) -> Dict:
    if yf_module is None:
        return {"ticker": ticker, "available": False}
    return fundamentals_fetcher.fetch_fundamentals_cached(yf_module, ticker, use_cache=use_cache)

def fetch_news(yf_module, ticker: str, limit: int = 5) -> Dict:
    try:
        news = yf_module.Ticker(ticker).news or []
    except Exception as exc:
        return {"available": False, "error": str(exc), "items": []}

    items = []
    for item in news[:limit]:
        content = item.get("content", item)
        title = content.get("title") or item.get("title")
        publisher = content.get("provider", {}).get("displayName") if isinstance(content.get("provider"), dict) else item.get("publisher")
        published = content.get("pubDate") or item.get("providerPublishTime")
        summary = content.get("summary") or content.get("description") or ""
        text = f"{title or ''} {summary or ''}".lower()
        bullish = sum(word in text for word in ("beat", "raise", "growth", "upgrade", "record", "profit", "strong"))
        bearish = sum(word in text for word in ("miss", "cut", "downgrade", "probe", "lawsuit", "weak", "risk"))
        items.append(
            {
                "headline": title,
                "source": publisher,
                "published": published,
                "summary": summary,
                "bullish_score": bullish,
                "bearish_score": bearish,
                "importance_score": min(100, 30 + 15 * (bullish + bearish)),
                "why_it_matters": _news_impact_text(bullish, bearish),
            }
        )
    return {"available": bool(items), "items": items}



def fetch_catalysts(fundamentals: Dict, news: Dict, technical: Dict, yf_module=None, ticker: Optional[str] = None) -> Dict:
    """Convert available Yahoo/news/technical fields into catalyst flags used by scoring."""
    if yf_module is not None and ticker:
        return fundamentals_fetcher.fetch_catalysts(yf_module, ticker, fundamentals=fundamentals, news=news, technical=technical)

    news_items = news.get("items", []) if news else []
    bullish_news = sum(item.get("bullish_score", 0) for item in news_items)
    bearish_news = sum(item.get("bearish_score", 0) for item in news_items)
    recommendation = str(fundamentals.get("recommendation") or "").lower()
    target = fundamentals.get("target_mean_price")
    forward_pe = fundamentals.get("forward_pe")
    eps_growth = fundamentals.get("eps_growth")
    revenue_growth = fundamentals.get("revenue_growth")
    insider_ownership = fundamentals.get("insider_ownership")
    institutional_ownership = fundamentals.get("institutional_ownership")

    analyst_revision = recommendation in {"buy", "strong_buy"} or bool(target and forward_pe and target > 0)
    earnings_surprise = bool((eps_growth and eps_growth > 0.10) or (revenue_growth and revenue_growth > 0.10))
    insider_buying = bool(insider_ownership and insider_ownership >= 0.05)
    sector_strength = bool(technical.get("price_above_sma50") and technical.get("price_above_sma200"))
    major_news = bool(news_items and max(item.get("importance_score", 0) for item in news_items) >= 60)
    guidance_change = bullish_news != bearish_news and abs(bullish_news - bearish_news) >= 2

    return {
        "earnings_surprise": earnings_surprise,
        "analyst_revision": analyst_revision,
        "insider_buying": insider_buying,
        "sector_strength": sector_strength,
        "major_news": major_news,
        "guidance_change": guidance_change,
        "bullish_news_score": bullish_news,
        "bearish_news_score": bearish_news,
        "institutional_ownership": institutional_ownership,
        "evidence": _catalyst_evidence(earnings_surprise, analyst_revision, insider_buying, sector_strength, major_news, guidance_change),
    }


def _catalyst_evidence(earnings_surprise, analyst_revision, insider_buying, sector_strength, major_news, guidance_change):
    evidence = []
    if earnings_surprise:
        evidence.append("growth/earnings trend is positive")
    if analyst_revision:
        evidence.append("analyst/target fields are supportive")
    if insider_buying:
        evidence.append("insider ownership is meaningful")
    if sector_strength:
        evidence.append("technical/sector backdrop is constructive")
    if major_news:
        evidence.append("recent news item has elevated importance")
    if guidance_change:
        evidence.append("news tone suggests a catalyst shift")
    return evidence

def technical_screen(analyzed: pd.DataFrame) -> Dict:
    latest = analyzed.iloc[-1]
    prior = analyzed.iloc[-2] if len(analyzed) > 1 else latest
    close = latest.get("Close")
    high_52w = latest.get("High_52W")
    macd_cross = (
        pd.notna(latest.get("MACD"))
        and pd.notna(latest.get("MACD_Signal"))
        and pd.notna(prior.get("MACD"))
        and pd.notna(prior.get("MACD_Signal"))
        and prior["MACD"] <= prior["MACD_Signal"]
        and latest["MACD"] > latest["MACD_Signal"]
    )
    breakout = pd.notna(close) and pd.notna(high_52w) and close >= high_52w * 0.98
    setup = []
    if breakout:
        setup.append("52-week high pressure")
    if macd_cross:
        setup.append("MACD bullish crossover")
    if latest.get("Close", 0) > latest.get("SMA50", np.inf) > latest.get("SMA200", np.inf):
        setup.append("trend continuation")
    if latest.get("RV", 0) >= 1.5:
        setup.append("volume surge")
    return {
        "rsi": _safe(latest.get("RSI")),
        "macd_cross": bool(macd_cross),
        "breakout": bool(breakout),
        "new_52w_high": bool(pd.notna(close) and pd.notna(high_52w) and close >= high_52w * 0.995),
        "relative_volume": _safe(latest.get("RV")),
        "price_above_sma50": bool(latest.get("Close", 0) > latest.get("SMA50", np.inf)),
        "price_above_sma200": bool(latest.get("Close", 0) > latest.get("SMA200", np.inf)),
        "setup_type": ", ".join(setup) if setup else "No clear setup",
        "support": intelligence_scoring.support_resistance_levels(analyzed).get("support"),
        "resistance": intelligence_scoring.support_resistance_levels(analyzed).get("resistance"),
    }


def screen_stock(row: Dict, filters: Dict) -> bool:
    fundamentals = row.get("fundamentals", {})
    technical = row.get("technical_screen", {})
    score = row.get("score", {}).get("final_score", 0)
    checks = [
        score >= filters.get("min_final_score", 0),
        _passes_min(fundamentals.get("revenue_growth"), filters.get("min_revenue_growth")),
        _passes_min(fundamentals.get("gross_margin"), filters.get("min_gross_margin")),
        _passes_max(fundamentals.get("forward_pe"), filters.get("max_forward_pe")),
        _passes_max(fundamentals.get("price_to_book"), filters.get("max_price_to_book")),
        _passes_min(technical.get("rsi"), filters.get("min_rsi")),
        _passes_max(technical.get("rsi"), filters.get("max_rsi")),
        not filters.get("require_price_above_sma200") or technical.get("price_above_sma200"),
        not filters.get("require_price_above_sma50") or technical.get("price_above_sma50"),
        not filters.get("require_52w_pressure") or technical.get("breakout"),
        _passes_min(technical.get("relative_volume"), filters.get("min_relative_volume")),
    ]
    return all(checks)


def build_stock_intelligence(
    ticker: str,
    analyzed: pd.DataFrame,
    benchmark_df: Optional[pd.DataFrame],
    market_payload: Dict,
    yf_module=None,
    sector_df: Optional[pd.DataFrame] = None,
    portfolio_context: Optional[Dict] = None,
) -> Dict:
    fundamentals = fetch_fundamentals(yf_module, ticker) if yf_module is not None else {"available": False}
    tech = technical_screen(analyzed)
    news = fetch_news(yf_module, ticker) if yf_module is not None else {"available": False, "items": []}
    catalysts = fetch_catalysts(fundamentals, news, tech, yf_module=yf_module, ticker=ticker)
    score = intelligence_scoring.final_stock_score(analyzed, benchmark_df=benchmark_df, sector_df=sector_df, fundamentals=fundamentals, catalysts=catalysts)
    why_payload = why_now.evaluate_why_now(ticker, analyzed, score, market_payload=market_payload)
    options_flow = {"available": False, "note": "Options flow scan requires a dedicated options-chain pass."}
    sentiment = {"available": False, "note": "Social sentiment sources are not configured."}
    earnings = earnings_snapshot(fundamentals, yf_module=yf_module, ticker=ticker)
    portfolio_fit = portfolio_fit_snapshot(ticker, portfolio_context)
    probability = probability_engine.probability_of_outperformance(score, fundamentals, tech, catalysts, market_payload)
    features = probability_engine.feature_snapshot(score, fundamentals, tech, catalysts)
    quant_model = quant_analytics.comprehensive_stock_analysis(analyzed, benchmark_df) if benchmark_df is not None else {}
    report = stock_report_text(ticker, score, fundamentals, tech, why_payload, news, portfolio_fit, probability=probability, quant_model=quant_model)
    return {
        "ticker": ticker,
        "generated_at": datetime.now().isoformat(),
        "score": score,
        "fundamentals": fundamentals,
        "technical_screen": tech,
        "earnings": earnings,
        "catalysts": catalysts,
        "analyst": analyst_snapshot(fundamentals),
        "insider_short_interest": insider_short_snapshot(fundamentals),
        "news": news,
        "sentiment": sentiment,
        "options_flow": options_flow,
        "portfolio_fit": portfolio_fit,
        "probability": probability,
        "features": features,
        "quant_model": quant_model,
        "why_now": why_payload,
        "report": report,
    }


def discover_stocks(intelligence_rows: Iterable[Dict], screeners: Optional[Dict] = None, screener_name: str = "quality_momentum") -> Dict:
    screeners = screeners or load_screeners()
    filters = screeners.get(screener_name, {})
    rows = list(intelligence_rows)
    matches = [row for row in rows if screen_stock(row, filters)]
    ranked = sorted(matches, key=lambda row: (row.get("why_now", {}).get("strength", 0), row.get("score", {}).get("final_score", 0)), reverse=True)
    all_ranked = sorted(rows, key=lambda row: row.get("score", {}).get("final_score", 0), reverse=True)
    return {
        "generated_at": datetime.now().isoformat(),
        "screener": screener_name,
        "filters": filters,
        "matches": ranked,
        "top_ranked": all_ranked[:20],
        "sector_rankings": sector_rankings(rows),
        "summary": discovery_summary(ranked, all_ranked),
    }


def sector_rankings(rows: Iterable[Dict]) -> Dict:
    buckets = {}
    for row in rows:
        sector = row.get("fundamentals", {}).get("sector") or "Unknown"
        buckets.setdefault(sector, []).append(row)
    rankings = []
    for sector, members in buckets.items():
        avg_score = np.mean([member.get("score", {}).get("final_score", 0) for member in members])
        leaders = sorted(members, key=lambda item: item.get("score", {}).get("final_score", 0), reverse=True)[:5]
        rankings.append(
            {
                "sector": sector,
                "average_score": round(float(avg_score), 2),
                "leader_count": len(members),
                "leaders": [{"ticker": item["ticker"], "score": item.get("score", {}).get("final_score", 0)} for item in leaders],
            }
        )
    return sorted(rankings, key=lambda item: item["average_score"], reverse=True)


def portfolio_fit_snapshot(ticker: str, portfolio_context: Optional[Dict]) -> Dict:
    if not portfolio_context:
        return {"available": False, "assessment": "Portfolio context unavailable."}
    report = portfolio_context.get("report", {})
    positions = {position.get("ticker") for position in portfolio_context.get("positions", [])}
    if ticker in positions:
        return {"available": True, "assessment": "Already held; monitor risk contribution and thesis quality."}
    sector = portfolio_context.get("sector_map", {}).get(ticker)
    sector_exposure = report.get("sector_exposure", {})
    sector_weight = sector_exposure.get(sector, 0) if sector else 0
    if sector_weight and sector_weight > 35:
        assessment = "May increase sector concentration risk."
    else:
        assessment = "Potential diversifier; evaluate correlation and volatility before adding."
    return {"available": True, "sector": sector, "current_sector_weight": sector_weight, "assessment": assessment}


def earnings_snapshot(fundamentals: Dict, yf_module=None, ticker: Optional[str] = None) -> Dict:
    if yf_module is not None and ticker:
        return fundamentals_fetcher.earnings_snapshot(yf_module, ticker, fundamentals)
    return {
        "next_earnings_date": fundamentals.get("next_earnings_date"),
        "surprise_tracking": "Unavailable from current data provider.",
        "risk_score": 50 if fundamentals.get("next_earnings_date") else 25,
        "note": "Earnings surprise streaks need historical earnings endpoint enrichment.",
    }

def analyst_snapshot(fundamentals: Dict) -> Dict:
    price = fundamentals.get("target_mean_price")
    return {
        "consensus": fundamentals.get("recommendation"),
        "target_mean_price": price,
        "target_high_price": fundamentals.get("target_high_price"),
        "target_low_price": fundamentals.get("target_low_price"),
        "number_of_analysts": fundamentals.get("number_of_analysts"),
    }


def insider_short_snapshot(fundamentals: Dict) -> Dict:
    return {
        "insider_ownership": fundamentals.get("insider_ownership"),
        "institutional_ownership": fundamentals.get("institutional_ownership"),
        "short_interest": fundamentals.get("short_interest"),
        "short_percent_float": fundamentals.get("short_percent_float"),
        "days_to_cover": fundamentals.get("days_to_cover"),
        "insider_transactions": "Unavailable from current data provider.",
    }


def stock_report_text(ticker: str, score: Dict, fundamentals: Dict, technical: Dict, why_payload: Dict, news: Dict, portfolio_fit: Dict, probability: Optional[Dict] = None, quant_model: Optional[Dict] = None) -> str:
    bull = []
    bear = []
    if score.get("categories", {}).get("momentum", 0) >= 60:
        bull.append("momentum and relative strength are constructive")
    if fundamentals.get("classification") in ("Quality Compounder", "Fundamentally Strong"):
        bull.append(f"fundamentals screen as {fundamentals.get('classification')}")
    if technical.get("breakout"):
        bull.append("price is pressing near breakout/high territory")
    if score.get("risk_level") == "High":
        bear.append("risk score is elevated")
    if fundamentals.get("forward_pe") and fundamentals.get("forward_pe") > 50:
        bear.append("valuation is demanding")
    if technical.get("relative_volume", 0) < 1:
        bear.append("volume confirmation is weak")
    top_news = news.get("items", [{}])[0].get("headline") if news.get("items") else "No major news item available"
    capm = (quant_model or {}).get("capm", {})
    factor = (quant_model or {}).get("factor_decomposition", {})
    capm_line = f"CAPM: Beta {capm.get('beta')} | Expected Return {capm.get('capm_expected_return')} | Alpha {capm.get('alpha')}" if capm else "CAPM: Unavailable"
    factor_line = f"Factor View: {factor.get('interpretation')}" if factor else "Factor View: Unavailable"
    return (
        f"Ticker: {ticker}\n"
        f"Rating: {score.get('rating')} | Score: {score.get('final_score')}/100 | Confidence: {score.get('confidence')}/100\n"
        f"Bull Case: {', '.join(bull) if bull else 'No decisive bull case yet.'}\n"
        f"Bear Case: {', '.join(bear) if bear else 'No major bear case detected from available data.'}\n"
        f"Technical Outlook: {technical.get('setup_type')} | Support: {technical.get('support')} | Resistance: {technical.get('resistance')}\n"
        f"Fundamental Outlook: {fundamentals.get('classification', 'Unavailable')} | Forward P/E: {fundamentals.get('forward_pe')}\n"
        f"News/Catalyst: {top_news}\n"
        f"Portfolio Fit: {portfolio_fit.get('assessment')}\n"
        f"Why Now: {why_payload.get('reason')} | Evidence: {why_payload.get('evidence')}\n"
        f"What Invalidates: {why_payload.get('invalidates', 'Setup invalidation unavailable')}"
    )


def discovery_summary(matches: Iterable[Dict], all_ranked: Iterable[Dict]) -> str:
    matches = list(matches)
    all_ranked = list(all_ranked)
    if matches:
        top = matches[0]
        return f"Top screened opportunity: {top['ticker']} with score {top.get('score', {}).get('final_score')}/100. Why Now: {top.get('why_now', {}).get('reason')}."
    if all_ranked:
        top = all_ranked[0]
        return f"No stocks passed the active screener. Highest ranked research candidate is {top['ticker']} at {top.get('score', {}).get('final_score')}/100."
    return "No stock intelligence rows were generated."



def _load_fundamentals_cache(ticker: str):
    path = _fundamentals_cache_path(ticker)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            payload = json.load(f)
        if time.time() - payload.get("cached_at", 0) <= FUNDAMENTALS_TTL_SECONDS:
            data = payload.get("data", {})
            data["from_cache"] = True
            return data
    except Exception:
        return None
    return None


def _save_fundamentals_cache(ticker: str, data: Dict):
    os.makedirs(FUNDAMENTALS_CACHE_DIR, exist_ok=True)
    with open(_fundamentals_cache_path(ticker), "w") as f:
        json.dump({"cached_at": time.time(), "data": data}, f, default=str)


def _fundamentals_cache_path(ticker: str) -> str:
    return os.path.join(FUNDAMENTALS_CACHE_DIR, f"{ticker.upper()}.json")

def _passes_min(value, threshold) -> bool:
    return True if threshold is None else value is not None and value >= threshold


def _passes_max(value, threshold) -> bool:
    return True if threshold is None else value is not None and value <= threshold


def _safe(value):
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _earnings_date(info: Dict):
    value = info.get("earningsDate") or info.get("earningsTimestamp")
    if isinstance(value, list) and value:
        return str(value[0])
    return str(value) if value else None


def _news_impact_text(bullish: int, bearish: int) -> str:
    if bullish > bearish:
        return "News appears more supportive of sentiment or growth expectations."
    if bearish > bullish:
        return "News may increase risk, valuation pressure, or negative sentiment."
    return "News impact is unclear; monitor price and volume reaction."
