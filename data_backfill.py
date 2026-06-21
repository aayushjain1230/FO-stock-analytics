"""Real-data backfills for watchlist intelligence, signal EV, and journal state."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable

import numpy as np
import pandas as pd

import database
import signal_validation
import trade_journal
import watchlist_intelligence

STATE_DIR = "state"
STOCK_REPORT_DIR = os.path.join(STATE_DIR, "stock_reports")


def _load_json(path, fallback=None):
    if fallback is None:
        fallback = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return fallback


def _safe_float(value, default=None):
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _fmt_money(value):
    value = _safe_float(value)
    return f"{value:.2f}" if value is not None else None


def load_stock_report(ticker: str) -> Dict:
    return _load_json(os.path.join(STOCK_REPORT_DIR, f"{ticker.upper()}.json"), {})


def latest_quant_rows() -> Dict[str, Dict]:
    payload = _load_json(os.path.join(STATE_DIR, "latest_quant_research.json"), {"tickers": []})
    return {str(row.get("ticker", "")).upper(): row for row in payload.get("tickers", []) if row.get("ticker")}


def refresh_watchlist_intelligence(watchlist: Iterable[str], overwrite_blank_only: bool = True) -> Dict:
    """Fill missing thesis/entry/stop/target fields from real saved stock reports."""
    payload = watchlist_intelligence.ensure_watchlist_records(watchlist)
    quant_rows = latest_quant_rows()
    updated = []
    skipped = []
    for ticker in [item.upper() for item in watchlist]:
        report = load_stock_report(ticker)
        row = quant_rows.get(ticker, {})
        record = payload.setdefault("tickers", {}).setdefault(ticker, {"ticker": ticker})
        if not report and not row:
            skipped.append({"ticker": ticker, "reason": "No stock report or quant row available yet."})
            continue

        score = report.get("score", {})
        tech_score = score.get("technical", {}) if isinstance(score.get("technical"), dict) else {}
        fundamentals = report.get("fundamentals", {})
        why = report.get("why_now", {})
        technical = report.get("technical_screen", {})
        company = fundamentals.get("company_name") or ticker
        final_score = score.get("final_score") or row.get("final_score") or row.get("quant_score")
        rating = score.get("rating") or row.get("rating") or row.get("quant_label")
        close = _safe_float(row.get("close"))
        support = _safe_float(technical.get("support") or tech_score.get("support"))
        resistance = _safe_float(technical.get("resistance") or tech_score.get("resistance"))
        risk_level = score.get("risk_level") or row.get("risk_level") or "Unknown"
        classification = fundamentals.get("classification") or score.get("fundamental", {}).get("classification") if isinstance(score.get("fundamental"), dict) else fundamentals.get("classification")
        why_reason = why.get("reason") or row.get("why_now") or "No active Why Now trigger"
        why_evidence = why.get("evidence") or score.get("explanation") or report.get("report") or "Evidence unavailable from latest report."

        thesis = (
            f"{company} is tracked as a {rating or 'research'} candidate with score {final_score or 'N/A'}/100. "
            f"Setup: {technical.get('setup_type', 'N/A')}. Fundamentals: {classification or 'N/A'}. "
            f"Why now: {why_reason}."
        )
        entry_zone = None
        if support and close:
            entry_zone = f"{min(support, close):.2f} - {max(support, close):.2f}"
        elif support:
            entry_zone = f"Near support around {support:.2f}"
        elif close:
            entry_zone = f"Around latest close {close:.2f}"
        stop_loss = support * 0.97 if support else close * 0.92 if close else None
        target_price = resistance if resistance else close * 1.12 if close else None
        time_horizon = "1-3 months" if why.get("send_alert") else "3-6 months"
        status = "active_watch" if why.get("send_alert") else "watching"
        risk_budget = 1.0 if str(risk_level).lower() == "high" else 2.0
        invalidation = (
            f"Invalid if price closes below stop near {_fmt_money(stop_loss)} or if the Why Now evidence fades: {why_evidence}"
            if stop_loss else f"Invalid if score/rating deteriorates or Why Now evidence fades: {why_evidence}"
        )

        fields = {
            "reason_added": f"Auto-filled from latest stock intelligence report generated {report.get('generated_at', 'unknown date')}.",
            "thesis": thesis,
            "entry_zone": entry_zone,
            "stop_loss": round(stop_loss, 2) if stop_loss else None,
            "target_price": round(target_price, 2) if target_price else None,
            "time_horizon": time_horizon,
            "status": status,
            "risk_budget_pct": risk_budget,
            "what_would_change_my_mind": invalidation,
        }
        changed = False
        for key, value in fields.items():
            if value is None:
                continue
            blank = record.get(key) in (None, "", "Thesis not set yet.", "Unspecified", "Define invalidation criteria.") or (key == "reason_added" and str(record.get(key, "")).startswith("Research watchlist candidate"))
            if blank or not overwrite_blank_only:
                record[key] = value
                changed = True
        if changed:
            record["updated_at"] = datetime.now().isoformat()
            updated.append(ticker)
    watchlist_intelligence.save_watchlist_intelligence(payload)
    report = watchlist_intelligence.build_watchlist_report(watchlist, list(quant_rows.values()))
    report["auto_refresh"] = {"updated": updated, "skipped": skipped, "source": "state/stock_reports + latest_quant_research"}
    with open(watchlist_intelligence.STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    return report


def _extract_close(raw, ticker: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    if isinstance(raw.columns, pd.MultiIndex):
        if ticker in raw.columns.get_level_values(0):
            df = raw[ticker].copy()
        elif ticker in raw.columns.get_level_values(-1):
            df = raw.xs(ticker, axis=1, level=-1).copy()
        else:
            return pd.DataFrame()
    else:
        df = raw.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(-1)
    return df.dropna(subset=["Close"]) if "Close" in df.columns else pd.DataFrame()


def backfill_historical_signals(yf_module, watchlist: Iterable[str], benchmark: str = "SPY", period: str = "2y") -> Dict:
    """Create real historical signal records from daily prices, then compute outcomes."""
    tickers = sorted({item.upper() for item in watchlist if item})
    if not tickers:
        return {"stored_signals": 0, "updated_outcomes": 0, "message": "Watchlist is empty."}
    database.initialize_database()
    raw = yf_module.download(tickers, period=period, interval="1d", group_by="ticker", threads=True, progress=False, auto_adjust=False)
    benchmark_raw = yf_module.download(benchmark, period=period, interval="1d", progress=False, auto_adjust=False)
    if benchmark_raw is not None and not benchmark_raw.empty:
        if isinstance(benchmark_raw.columns, pd.MultiIndex):
            if "Close" in benchmark_raw.columns.get_level_values(-1):
                benchmark_df = benchmark_raw.xs("Close", axis=1, level=-1)
                if isinstance(benchmark_df, pd.DataFrame):
                    benchmark_df = benchmark_df.iloc[:, [0]].rename(columns={benchmark_df.columns[0]: "Close"})
                else:
                    benchmark_df = benchmark_df.to_frame("Close")
            else:
                benchmark_df = pd.DataFrame()
        elif "Close" in benchmark_raw.columns:
            benchmark_df = benchmark_raw[["Close"]].dropna()
        else:
            benchmark_df = pd.DataFrame()
    else:
        benchmark_df = pd.DataFrame()
    price_lookup = {}
    stored = 0
    by_type = {}
    for ticker in tickers:
        df = _extract_close(raw, ticker)
        if df.empty or len(df) < 260:
            continue
        price_lookup[ticker] = df
        close = pd.to_numeric(df["Close"], errors="coerce")
        volume = pd.to_numeric(df.get("Volume", pd.Series(index=df.index, dtype=float)), errors="coerce")
        sma50 = close.rolling(50).mean()
        sma200 = close.rolling(200).mean()
        vol_avg = volume.rolling(20).mean()
        high_252 = close.rolling(252).max().shift(1)
        bench_close = benchmark_df["Close"].reindex(close.index).ffill() if not benchmark_df.empty else None
        rs_60 = close.pct_change(60) - bench_close.pct_change(60) if bench_close is not None else pd.Series(index=close.index, dtype=float)
        report = load_stock_report(ticker)
        sector = report.get("fundamentals", {}).get("sector") or "Unknown"
        for idx in range(201, len(df) - 126):
            date = close.index[idx]
            if pd.isna(close.iloc[idx]) or pd.isna(sma50.iloc[idx]) or pd.isna(sma200.iloc[idx]):
                continue
            signals = []
            if close.iloc[idx - 1] <= sma50.iloc[idx - 1] and close.iloc[idx] > sma50.iloc[idx] and close.iloc[idx] > sma200.iloc[idx]:
                signals.append(("sma50_reclaim", 62, "Price reclaimed SMA50 while above SMA200."))
            if pd.notna(high_252.iloc[idx]) and close.iloc[idx] >= high_252.iloc[idx] * 0.995 and volume.iloc[idx] > vol_avg.iloc[idx] * 1.2:
                signals.append(("high_volume_52w_breakout", 72, "Price pressed a 52-week high with above-average volume."))
            if pd.notna(rs_60.iloc[idx]) and rs_60.iloc[idx] > 0.05 and close.iloc[idx] > sma50.iloc[idx] > sma200.iloc[idx]:
                signals.append(("relative_strength_leadership", 68, "60-day return outperformed SPY by more than 5 percentage points."))
            for signal_type, score, reason in signals[:2]:
                why_now = {
                    "reason": reason,
                    "evidence": f"Historical backfill generated from real daily OHLCV on {date.date().isoformat()}.",
                    "invalidates": "Close back below SMA50/SMA200 or failed breakout follow-through.",
                }
                database.store_signal(ticker, date.date().isoformat(), signal_type, float(close.iloc[idx]), score, "Historical", sector, why_now, score)
                stored += 1
                by_type[signal_type] = by_type.get(signal_type, 0) + 1
    updated = signal_validation.update_signal_outcomes(price_lookup, benchmark_df=benchmark_df)
    return {"stored_signals_attempted": stored, "stored_by_type": by_type, "updated_outcomes": updated, "tickers": tickers, "period": period}


def sync_trade_journal_from_portfolio(yf_module, portfolio_path: str = "portfolio.json") -> Dict:
    """Add real portfolio/current-position context to the trade journal state without fabricating trades."""
    portfolio = _load_json(portfolio_path, {"positions": []})
    positions = portfolio.get("positions", [])
    tickers = sorted({p.get("ticker", "").upper() for p in positions if p.get("ticker")})
    price_map = {}
    if tickers:
        raw = yf_module.download(tickers, period="5d", interval="1d", group_by="ticker", threads=True, progress=False, auto_adjust=False)
        for ticker in tickers:
            df = _extract_close(raw, ticker)
            if not df.empty:
                price_map[ticker] = float(df["Close"].dropna().iloc[-1])
    base = trade_journal.summarize_trades()
    model_positions = []
    actual_count = 0
    for pos in positions:
        ticker = str(pos.get("ticker", "")).upper()
        current = price_map.get(ticker)
        shares = _safe_float(pos.get("shares"))
        cost_basis = _safe_float(pos.get("cost_basis"))
        market_value = shares * current if shares is not None and current is not None else None
        unrealized = (current - cost_basis) * shares if shares is not None and cost_basis is not None and current is not None else None
        return_pct = current / cost_basis - 1 if cost_basis and current else None
        if shares is not None and cost_basis is not None:
            actual_count += 1
        model_positions.append({
            "ticker": ticker,
            "weight": pos.get("weight"),
            "sector": pos.get("sector"),
            "shares": shares,
            "cost_basis": cost_basis,
            "current_price": current,
            "market_value": market_value,
            "unrealized_pnl": unrealized,
            "unrealized_return_pct": return_pct,
            "data_quality": "actual_position" if shares is not None and cost_basis is not None else "model_weight_only",
        })
    base["portfolio_positions"] = model_positions
    base["portfolio_position_count"] = len(model_positions)
    base["actual_position_count"] = actual_count
    base["journal_data_note"] = (
        "Actual trade P&L requires real shares, cost_basis, and exits in trade_journal.json/portfolio.json. "
        "Current output includes real market prices and model weights where actual trade data is missing."
    )
    with open(trade_journal.STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(base, f, indent=2, default=str)
    return base
