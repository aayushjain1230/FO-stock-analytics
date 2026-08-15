"""Central market-data service with caching, normalization, and graceful errors."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import pandas as pd

try:
    import yfinance as yf
except Exception:  # pragma: no cover - exercised when dependency unavailable
    yf = None

from app.models.snapshot import MarketSnapshot
from app.models.stock import StockSnapshot
from app.services.watchlist import normalize_ticker

PRICE_TTL_SECONDS = 300
FUNDAMENTAL_TTL_SECONDS = 86400
_PRICE_CACHE: dict[tuple, tuple[float, Any]] = {}
_FUNDAMENTAL_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _cache_get(cache: dict, key: tuple | str, ttl: int) -> Any | None:
    item = cache.get(key)
    if not item:
        return None
    timestamp, value = item
    if time.time() - timestamp <= ttl:
        return value
    return None


def _cache_set(cache: dict, key: tuple | str, value: Any) -> Any:
    cache[key] = (time.time(), value)
    return value


def normalize_download(raw: pd.DataFrame, tickers: list[str]) -> dict[str, pd.DataFrame]:
    """Normalize yfinance single or MultiIndex responses into per-ticker frames."""
    if raw is None or raw.empty:
        return {ticker: pd.DataFrame() for ticker in tickers}
    if isinstance(raw.columns, pd.MultiIndex):
        output: dict[str, pd.DataFrame] = {}
        first_level = set(str(x).upper() for x in raw.columns.get_level_values(0))
        for ticker in tickers:
            try:
                if ticker in first_level:
                    output[ticker] = raw[ticker].dropna(how="all")
                else:
                    output[ticker] = raw.xs(ticker, axis=1, level=1).dropna(how="all")
            except Exception:
                output[ticker] = pd.DataFrame()
        return output
    if len(tickers) == 1:
        return {tickers[0]: raw.dropna(how="all")}
    return {ticker: pd.DataFrame() for ticker in tickers}


def fetch_price_history(tickers: list[str], period: str = "6mo", interval: str = "1d", force_refresh: bool = False) -> dict[str, pd.DataFrame]:
    """Fetch price history in one batch with cache and per-ticker failure isolation."""
    tickers = [normalize_ticker(t) for t in tickers]
    key = (tuple(tickers), period, interval)
    if not force_refresh:
        cached = _cache_get(_PRICE_CACHE, key, PRICE_TTL_SECONDS)
        if cached is not None:
            return cached
    if yf is None:
        return {ticker: pd.DataFrame() for ticker in tickers}
    try:
        raw = yf.download(tickers, period=period, interval=interval, group_by="ticker", threads=True, progress=False, timeout=20, auto_adjust=False)
        return _cache_set(_PRICE_CACHE, key, normalize_download(raw, tickers))
    except Exception:
        return {ticker: pd.DataFrame() for ticker in tickers}


def fetch_fundamentals(ticker: str, force_refresh: bool = False) -> dict[str, Any]:
    """Fetch basic company fields with long TTL and safe missing states."""
    ticker = normalize_ticker(ticker)
    if not force_refresh:
        cached = _cache_get(_FUNDAMENTAL_CACHE, ticker, FUNDAMENTAL_TTL_SECONDS)
        if cached is not None:
            return cached
    if yf is None:
        return {}
    try:
        info = yf.Ticker(ticker).get_info() or {}
    except Exception:
        info = {}
    data = {
        "company_name": info.get("shortName") or info.get("longName"),
        "market_cap": info.get("marketCap"),
        "revenue_growth": info.get("revenueGrowth"),
        "earnings_growth": info.get("earningsGrowth"),
        "profit_margin": info.get("profitMargins"),
        "debt_to_equity": info.get("debtToEquity"),
        "free_cash_flow": info.get("freeCashflow"),
        "forward_pe": info.get("forwardPE"),
        "next_earnings_date": _extract_earnings_date(info),
    }
    return _cache_set(_FUNDAMENTAL_CACHE, ticker, data)


def _extract_earnings_date(info: dict[str, Any]) -> str | None:
    value = info.get("earningsDate") or info.get("earningsTimestamp")
    if isinstance(value, list) and value:
        value = value[0]
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value).date().isoformat()
        if value is not None:
            return str(value)
    except Exception:
        return None
    return None


def snapshot_from_history(ticker: str, history: pd.DataFrame, fundamentals: dict[str, Any]) -> StockSnapshot:
    """Convert raw price/fundamental data into a normalized stock snapshot."""
    ticker = normalize_ticker(ticker)
    if history is None or history.empty or "Close" not in history:
        return StockSnapshot(ticker=ticker, company_name=fundamentals.get("company_name"), price=None, daily_change_pct=None, five_day_change_pct=None, twenty_day_change_pct=None, volume_ratio=None, error="No price data returned")
    close = pd.to_numeric(history["Close"], errors="coerce").dropna()
    volume = pd.to_numeric(history.get("Volume"), errors="coerce").dropna() if "Volume" in history else pd.Series(dtype=float)
    if close.empty:
        return StockSnapshot(ticker=ticker, company_name=fundamentals.get("company_name"), price=None, daily_change_pct=None, five_day_change_pct=None, twenty_day_change_pct=None, volume_ratio=None, error="No valid close prices")
    daily = _return_over(close, 1)
    five = _return_over(close, 5)
    twenty = _return_over(close, 20)
    volume_ratio = None
    if len(volume) >= 21 and volume.tail(20).mean() > 0:
        volume_ratio = float(volume.iloc[-1] / volume.tail(20).mean())
    return StockSnapshot(
        ticker=ticker,
        company_name=fundamentals.get("company_name"),
        price=float(close.iloc[-1]),
        daily_change_pct=daily,
        five_day_change_pct=five,
        twenty_day_change_pct=twenty,
        volume_ratio=volume_ratio,
        market_cap=fundamentals.get("market_cap"),
        revenue_growth=fundamentals.get("revenue_growth"),
        earnings_growth=fundamentals.get("earnings_growth"),
        profit_margin=fundamentals.get("profit_margin"),
        debt_to_equity=fundamentals.get("debt_to_equity"),
        free_cash_flow=fundamentals.get("free_cash_flow"),
        forward_pe=fundamentals.get("forward_pe"),
        next_earnings_date=fundamentals.get("next_earnings_date"),
        updated_at=datetime.utcnow(),
    )


def _return_over(close: pd.Series, days: int) -> float | None:
    if len(close) <= days:
        return None
    try:
        return float(close.iloc[-1] / close.iloc[-days - 1] - 1)
    except Exception:
        return None


def fetch_snapshots(tickers: list[str], period: str = "6mo", interval: str = "1d", force_refresh: bool = False) -> dict[str, StockSnapshot]:
    """Fetch normalized snapshots for a watchlist; one failure does not stop others."""
    histories = fetch_price_history(tickers, period=period, interval=interval, force_refresh=force_refresh)
    snapshots: dict[str, StockSnapshot] = {}
    for ticker in tickers:
        fundamentals = fetch_fundamentals(ticker, force_refresh=force_refresh)
        snapshots[ticker] = snapshot_from_history(ticker, histories.get(ticker, pd.DataFrame()), fundamentals)
    return snapshots


def fetch_market_snapshot(force_refresh: bool = False) -> MarketSnapshot:
    """Fetch a simple market overview for SPY and QQQ."""
    histories = fetch_price_history(["SPY", "QQQ"], period="10d", interval="1d", force_refresh=force_refresh)
    spy = snapshot_from_history("SPY", histories.get("SPY", pd.DataFrame()), {})
    qqq = snapshot_from_history("QQQ", histories.get("QQQ", pd.DataFrame()), {})
    changes = [value for value in [spy.daily_change_pct, qqq.daily_change_pct] if value is not None]
    if not changes:
        return MarketSnapshot(warnings=["Market index data unavailable."])
    avg = sum(changes) / len(changes)
    condition = "Supportive" if avg > 0.003 else "Weak" if avg < -0.003 else "Mixed"
    explanation = f"The broad market is {condition.lower()} today based on SPY and Nasdaq-tracking QQQ movement."
    return MarketSnapshot(sp500_change_pct=spy.daily_change_pct, nasdaq_change_pct=qqq.daily_change_pct, condition=condition, explanation=explanation, updated_at=datetime.utcnow())
