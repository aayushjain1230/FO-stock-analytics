import argparse
import copy
import hashlib
import json
import os
from datetime import datetime
from io import StringIO
from typing import Dict

import pandas as pd
import requests

try:
    import pytz
except ModuleNotFoundError:
    pytz = None

try:
    import pandas_market_calendars as mcal
except ModuleNotFoundError:
    mcal = None

try:
    import yfinance as yf
except ModuleNotFoundError:
    yf = None
from dotenv import load_dotenv

import plotting
import database
import intelligence_scoring
import market_regime
import options_analytics
import portfolio_engine
import quant_analytics
import quant_dashboard
import research_reports
import scoring
import signal_validation
import state_manager
import stock_discovery
import telegram_notifier
import why_now
try:
    from indicators import calculate_metrics, get_market_regime_label
except ModuleNotFoundError:
    calculate_metrics = None
    get_market_regime_label = None
from logger_config import setup_logger
from utils import cache_result, retry_on_failure


load_dotenv()

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(log_level=log_level)

WATCHLIST_FILE = "watchlist.json"
STATE_DIR = "state"
HASH_FILE = os.path.join(STATE_DIR, "last_report_hash.json")
COMPARISON_FILE = os.path.join(STATE_DIR, "latest_comparison.json")
QUANT_RESEARCH_FILE = os.path.join(STATE_DIR, "latest_quant_research.json")
PORTFOLIO_REPORT_FILE = os.path.join(STATE_DIR, "latest_portfolio_report.json")
STOCK_DISCOVERY_FILE = os.path.join(STATE_DIR, "latest_stock_discovery.json")
STOCK_REPORT_DIR = os.path.join(STATE_DIR, "stock_reports")

DEFAULT_CONFIG = {
    "benchmark": "SPY",
    "settings": {
        "period": "1y",
        "interval": "1d",
        "research_interval": "1d",
        "use_intraday_research": False,
        "minimum_history_days": 252,
        "sma_trend": 20,
        "sma_fast": 50,
        "sma_slow": 200,
        "rsi_period": 14,
        "rsi_leader_threshold": 50,
        "rsi_weekly_breakdown_threshold": 40,
        "rsi_monthly_breakout_threshold": 40,
        "atr_period": 14,
        "atr_extension_threshold": 3.0,
        "relative_strength_period": 50,
        "relative_strength_signal_period": 20,
        "volume_average_period": 20,
        "relative_volume_watch_threshold": 1.5,
        "relative_volume_alert_threshold": 2.0,
        "fifty_two_week_window": 252,
        "respect_market_hours": True,
        "interesting_move_threshold_pct": 2.0,
        "compact_mode_max_tickers": 2,
        "risk_free_rate": 0.045,
        "why_now_min_score": 50,
    },
    "telegram": {
        "enabled": True,
        "send_sector_summary": True,
    },
}

os.makedirs("plots", exist_ok=True)
os.makedirs("logs", exist_ok=True)
os.makedirs("cache", exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)


def _deep_merge(base, override):
    merged = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_watchlist_data():
    if not os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE, "w") as f:
            json.dump(["SPY", "QQQ"], f)
        return ["SPY", "QQQ"]
    try:
        with open(WATCHLIST_FILE, "r") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def save_watchlist_data(tickers):
    tickers = sorted(set(t.upper() for t in tickers))
    with open(WATCHLIST_FILE, "w") as f:
        json.dump(tickers, f, indent=4)


def manage_cli_updates(add_list=None, remove_list=None):
    current = load_watchlist_data()
    changed = False

    if add_list:
        for ticker in add_list:
            ticker = ticker.upper()
            if ticker not in current:
                current.append(ticker)
                changed = True
                logger.info(f"Added {ticker} to watchlist")

    if remove_list:
        for ticker in remove_list:
            ticker = ticker.upper()
            if ticker in current:
                current.remove(ticker)
                changed = True
                logger.info(f"Removed {ticker} from watchlist")

    if changed:
        save_watchlist_data(current)


def load_config():
    path = os.path.join("config", "config.json")
    if not os.path.exists(path):
        return copy.deepcopy(DEFAULT_CONFIG)

    try:
        with open(path, "r") as f:
            return _deep_merge(DEFAULT_CONFIG, json.load(f))
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return copy.deepcopy(DEFAULT_CONFIG)


def is_market_open():
    """Check whether the NYSE is currently open."""
    if mcal is None or pytz is None:
        raise RuntimeError("pandas_market_calendars and pytz are required for market-hours checks. Install requirements or use --force for research runs.")
    nyse = mcal.get_calendar("NYSE")
    now_utc = datetime.now(pytz.utc)
    now_est = now_utc.astimezone(pytz.timezone("America/New_York"))
    today = now_est.date().isoformat()
    schedule = nyse.schedule(start_date=today, end_date=today)

    if schedule.empty:
        return False

    market_open = schedule.iloc[0].market_open
    market_close = schedule.iloc[0].market_close
    return market_open <= now_utc <= market_close


def should_send_report(content):
    """Prevent duplicate summary messages by hashing the payload."""
    content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()

    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, "r") as f:
            try:
                state = json.load(f)
                if state.get("hash") == content_hash:
                    return False
            except Exception:
                pass

    with open(HASH_FILE, "w") as f:
        json.dump({"hash": content_hash, "timestamp": str(datetime.now())}, f)

    return True


def save_comparison_snapshot(rows):
    os.makedirs(os.path.dirname(COMPARISON_FILE), exist_ok=True)
    payload = {
        "generated_at": datetime.now().isoformat(),
        "tickers": rows,
    }
    with open(COMPARISON_FILE, "w") as f:
        json.dump(payload, f, indent=2)


@cache_result(cache_key="sp500_sectors", ttl_seconds=86400)
@retry_on_failure(max_retries=3, delay=2)
def get_sp500_sectors() -> Dict[str, str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    table = pd.read_html(StringIO(response.text))[0]
    table["Symbol"] = table["Symbol"].str.replace(".", "-", regex=False)
    return dict(zip(table["Symbol"], table["GICS Sector"]))


def _normalize_benchmark_data(benchmark_raw):
    if isinstance(benchmark_raw.columns, pd.MultiIndex):
        if "Close" in benchmark_raw.columns.get_level_values(1):
            benchmark_data = benchmark_raw.xs("Close", level=1, axis=1)
        else:
            benchmark_data = benchmark_raw.iloc[:, 0]
    else:
        benchmark_data = benchmark_raw["Close"]
    return benchmark_data.to_frame("Close").dropna()


def _build_comparison_row(ticker, latest, rating, quant_payload=None, intelligence_payload=None, why_now_payload=None):
    quant_payload = quant_payload or {}
    intelligence_payload = intelligence_payload or {}
    why_now_payload = why_now_payload or {}
    close_price = latest.get("Close")
    sma200 = latest.get("SMA200")
    mrs = latest.get("MRS")
    weekly_rsi = latest.get("RSI_Weekly")
    monthly_rsi = latest.get("RSI_Monthly")

    return {
        "ticker": ticker,
        "score": int(rating.get("score", 0)),
        "rating": rating.get("rating", "N/A"),
        "close": round(float(close_price), 2) if pd.notna(close_price) else None,
        "weekly_rsi": round(float(weekly_rsi), 2) if pd.notna(weekly_rsi) else None,
        "monthly_rsi": round(float(monthly_rsi), 2) if pd.notna(monthly_rsi) else None,
        "above_sma200": bool(pd.notna(close_price) and pd.notna(sma200) and close_price >= sma200),
        "rs_status": "leading" if pd.notna(mrs) and mrs > 0 else "lagging",
        "relative_volume": rating.get("metrics", {}).get("rel_volume", "N/A"),
        "quant_score": quant_payload.get("quant_score", {}).get("score"),
        "quant_label": quant_payload.get("quant_score", {}).get("label"),
        "annualized_volatility": quant_payload.get("risk", {}).get("annualized_volatility"),
        "max_drawdown": quant_payload.get("risk", {}).get("maximum_drawdown"),
        "sharpe_ratio": quant_payload.get("risk", {}).get("sharpe_ratio"),
        "volatility_regime": quant_payload.get("volatility_regime"),
        "final_score": intelligence_payload.get("final_score"),
        "confidence": intelligence_payload.get("confidence"),
        "risk_level": intelligence_payload.get("risk_level"),
        "why_now": why_now_payload.get("reason"),
        "why_now_strength": why_now_payload.get("strength"),
        "what_invalidates": why_now_payload.get("invalidates"),
    }


def _is_interesting_ticker(alerts, daily_change, rating, config):
    settings = config.get("settings", {})
    move_threshold = float(settings.get("interesting_move_threshold_pct", 2.0))
    has_alert = any("Initial data recorded" not in alert for alert in (alerts or []))
    has_major_score = rating.get("score", 0) >= 80
    has_notable_move = abs(daily_change) >= move_threshold
    return has_alert or has_major_score or has_notable_move


def _build_run_summary(interesting_rows, market_regime, watchlist_count, config):
    settings = config.get("settings", {})
    sorted_rows = sorted(interesting_rows, key=lambda row: row["daily_change"])
    biggest_down = sorted_rows[0] if sorted_rows else None
    biggest_up = sorted_rows[-1] if sorted_rows else None
    return {
        "market_regime": market_regime,
        "interesting_count": len(interesting_rows),
        "watchlist_count": watchlist_count,
        "biggest_up": biggest_up,
        "biggest_down": biggest_down,
        "compact_mode_max_tickers": int(settings.get("compact_mode_max_tickers", 2)),
    }




def _yahoo_safe_period(period, interval):
    """Clamp Yahoo Finance periods to ranges supported for intraday candles."""
    intraday_limits = {
        "1m": "7d",
        "2m": "60d",
        "5m": "60d",
        "15m": "60d",
        "30m": "60d",
        "60m": "730d",
        "90m": "60d",
        "1h": "730d",
    }
    interval = str(interval or "1d").lower()
    period = str(period or "1y").lower()
    if interval not in intraday_limits:
        return period

    limit = intraday_limits[interval]
    if _period_to_days(period) > _period_to_days(limit):
        logger.warning(f"Yahoo only supports interval={interval} for about {limit}; using period={limit} instead of {period}.")
        return limit
    return period


def _period_to_days(period):
    period = str(period or "0d").lower().strip()
    if period.endswith("mo"):
        number, unit = period[:-2], "mo"
    else:
        number, unit = period[:-1], period[-1:]
    try:
        value = int(number)
    except Exception:
        return 10_000
    if unit == "d":
        return value
    if unit == "w":
        return value * 7
    if unit == "mo":
        return value * 30
    if unit == "y":
        return value * 365
    return 10_000


def _research_download_params(settings, period_key="period", default_period="1y"):
    """Use stable daily candles for scoring unless intraday research is explicitly enabled."""
    period = settings.get(period_key, default_period)
    configured_interval = str(settings.get("interval", "1d")).lower()
    research_interval = str(settings.get("research_interval", "1d")).lower()

    if settings.get("use_intraday_research", False):
        interval = configured_interval
    else:
        interval = research_interval or "1d"
        if configured_interval not in ("1d", "1wk", "1mo") and interval == "1d":
            logger.warning(
                f"Config interval={configured_interval} is intraday. Running the bot every 15 minutes does not require intraday candles; using interval=1d for research scoring."
            )

    return period, interval, _yahoo_safe_period(period, interval)

def _download_context(symbol_map, period, interval):
    if yf is None:
        return {}
    symbols = list(symbol_map.values())
    safe_period = _yahoo_safe_period(period, interval)
    raw = yf.download(symbols, period=safe_period, interval=interval, group_by="ticker", threads=True, progress=False, auto_adjust=False)
    data = {}
    for name, symbol in symbol_map.items():
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                if symbol in raw.columns.get_level_values(0):
                    frame = raw[symbol].copy().dropna(subset=["Close"])
                else:
                    continue
            else:
                frame = raw.copy().dropna(subset=["Close"])
            frame.name = name
            data[name] = frame
        except Exception:
            continue
    return data


def _build_market_intelligence(period, interval):
    index_data = _download_context(market_regime.INDEX_SYMBOLS, period, interval)
    sector_symbol_map = {name: symbol for name, symbol in market_regime.SECTOR_ETFS.items()}
    sector_data = _download_context(sector_symbol_map, period, interval)
    payload = market_regime.classify_market(index_data, sector_data)
    database.store_market_regime(datetime.now().date().isoformat(), payload)
    return payload, sector_data


def _combine_rating(base_rating, intelligence_payload, why_payload):
    rating = copy.deepcopy(base_rating)
    final_score = intelligence_payload.get("final_score")
    if final_score is not None:
        rating["score"] = int(round(final_score))
        rating["rating"] = intelligence_payload.get("rating", rating.get("rating", "N/A"))
    rating["confidence"] = intelligence_payload.get("confidence")
    rating["risk_level"] = intelligence_payload.get("risk_level")
    rating["why_now"] = why_payload
    rating.setdefault("metrics", {})["why_now"] = why_payload.get("reason", "No clear Why Now trigger")
    return rating

def run_analytics_engine(force=False):
    logger.info("=" * 60)
    logger.info("Jain Family Office: Market Intelligence Engine v2")
    logger.info("=" * 60)

    if yf is None or calculate_metrics is None or get_market_regime_label is None:
        raise RuntimeError("Market analytics require yfinance, pandas_market_calendars, and pandas-ta. Install compatible dependencies before running scans.")

    database.initialize_database()

    config = load_config()
    settings = config.get("settings", {})

    if not force and settings.get("respect_market_hours", True) and not is_market_open():
        logger.info("Market is closed. Cycle terminated to avoid duplicate notifications.")
        return

    watchlist = [ticker.upper() for ticker in load_watchlist_data()]
    benchmark_symbol = config.get("benchmark", "SPY")
    sector_map = get_sp500_sectors()
    scan_list = sorted(set(watchlist + list(sector_map.keys())))

    logger.info(f"Scanning {len(scan_list)} total tickers (watchlist + S&P 500)")

    period, interval, download_period = _research_download_params(settings, period_key="period", default_period="1y")
    minimum_history_days = int(settings.get("minimum_history_days", 252))
    risk_free_rate = float(settings.get("risk_free_rate", 0.045))

    batch_raw = yf.download(
        scan_list,
        period=download_period,
        interval=interval,
        group_by="ticker",
        threads=True,
        progress=False,
        auto_adjust=False,
    )
    benchmark_raw = yf.download(
        benchmark_symbol,
        period=download_period,
        interval=interval,
        progress=False,
        auto_adjust=False,
    )

    if benchmark_raw.empty:
        logger.critical("CRITICAL: Failed to download benchmark data. Aborting cycle.")
        return

    benchmark_data = _normalize_benchmark_data(benchmark_raw)
    market_payload, sector_data = _build_market_intelligence(download_period, interval)
    market_regime_label = market_payload.get("regime") or get_market_regime_label(benchmark_data, config=config)
    logger.info(f"Detected Market Regime: {market_regime_label} | Health {market_payload.get('health_score')}/100")

    previous_state = state_manager.load_previous_state()
    new_state = {}
    watchlist_data_for_plot = {}
    watchlist_reports = []
    comparison_rows = []
    interesting_rows = []
    sector_performance_tracker = {}
    sector_scoring_tracker = {}

    for ticker in scan_list:
        try:
            if ticker not in batch_raw.columns.levels[0]:
                continue

            df = batch_raw[ticker].copy().dropna(subset=["Close"])
            if len(df) < minimum_history_days:
                continue

            analyzed = calculate_metrics(df, benchmark_data, config=config)
            database.upsert_stock(ticker, sector=sector_map.get(ticker, "Other"))
            database.store_price_history(ticker, df)
            database.store_technical_metrics(ticker, analyzed)

            base_rating = scoring.generate_rating(analyzed, config=config)
            quant_payload = quant_analytics.comprehensive_stock_analysis(analyzed, benchmark_data, risk_free_rate=risk_free_rate)
            sector_df = sector_data.get(sector_map.get(ticker, ""))
            intelligence_payload = intelligence_scoring.final_stock_score(analyzed, benchmark_df=benchmark_data, sector_df=sector_df)
            score_date = analyzed.index[-1].date().isoformat() if hasattr(analyzed.index[-1], "date") else str(analyzed.index[-1])
            previous_scores = database.recent_scores(ticker, limit=1)
            why_payload = why_now.evaluate_why_now(ticker, analyzed, intelligence_payload, previous_scores=previous_scores, market_payload=market_payload)
            database.store_stock_score(ticker, score_date, intelligence_payload)
            rating = _combine_rating(base_rating, intelligence_payload, why_payload)
            alerts = state_manager.get_ticker_alerts(ticker, analyzed, previous_state, config=config)
            latest = analyzed.iloc[-1]

            new_state = state_manager.update_ticker_state(ticker, analyzed, new_state, config=config)

            if len(df) > 1:
                daily_change = ((df["Close"].iloc[-1] - df["Close"].iloc[-2]) / df["Close"].iloc[-2]) * 100
            else:
                daily_change = 0.0

            sector = sector_map.get(ticker, "Other")
            sector_performance_tracker.setdefault(sector, []).append(daily_change)
            sector_scoring_tracker.setdefault(sector, []).append((ticker, rating["score"]))

            if ticker in watchlist:
                comparison_row = _build_comparison_row(ticker, latest, rating, quant_payload=quant_payload, intelligence_payload=intelligence_payload, why_now_payload=why_payload)
                comparison_row["daily_change"] = round(float(daily_change), 2)
                comparison_row["market_regime"] = market_regime_label
                comparison_row["research_note"] = research_reports.stock_research_note(ticker, latest, rating, quant_payload)
                comparison_rows.append(comparison_row)
                watchlist_data_for_plot[ticker] = analyzed

                if why_payload.get("send_alert") and (_is_interesting_ticker(alerts, daily_change, rating, config) or rating.get("score", 0) >= int(settings.get("why_now_min_score", 50))):
                    database.store_signal(
                        ticker=ticker,
                        date=score_date,
                        signal_type=why_now.signal_type_from_why_now(why_payload),
                        entry_price=latest.get("Close"),
                        score=rating.get("score", 0),
                        market_regime=market_regime_label,
                        sector=sector,
                        why_now=why_payload,
                        confidence=rating.get("confidence") or 0,
                    )
                    interesting_rows.append(comparison_row)
                    watchlist_reports.append(
                        telegram_notifier.format_ticker_report(
                            ticker,
                            alerts,
                            latest,
                            rating,
                            daily_change=daily_change,
                        )
                    )

        except Exception as e:
            logger.warning(f"Error processing ticker {ticker}: {e}")

    final_sector_stats = {}
    for sector_name, changes in sector_performance_tracker.items():
        if not changes:
            continue

        avg_change = sum(changes) / len(changes)
        sector_ranks = sector_scoring_tracker.get(sector_name, [])
        if sector_ranks:
            sector_ranks.sort(key=lambda item: item[1])
            top_stock = sector_ranks[-1][0]
            bottom_stock = sector_ranks[0][0]
        else:
            top_stock = "N/A"
            bottom_stock = "N/A"

        final_sector_stats[sector_name] = {
            "change": avg_change,
            "top": top_stock,
            "bottom": bottom_stock,
        }

    comparison_rows = sorted(comparison_rows, key=lambda row: row["ticker"])
    save_comparison_snapshot(comparison_rows)

    run_summary = _build_run_summary(interesting_rows, market_regime_label, len(watchlist), config)
    run_summary["market_health_score"] = market_payload.get("health_score")
    run_summary["risk_environment"] = market_payload.get("risk_environment")
    run_summary["buy_environment"] = market_payload.get("buy_environment")

    telegram_payload = json.dumps(
        {
            "watchlist_reports": watchlist_reports,
            "run_summary": run_summary,
            "sector_summary": final_sector_stats if len(watchlist_reports) > run_summary["compact_mode_max_tickers"] else {},
            "market_regime": market_regime_label,
        },
        sort_keys=True,
    )
    if watchlist_reports and should_send_report(telegram_payload):
        sector_summary = final_sector_stats if config.get("telegram", {}).get("send_sector_summary", True) else {}
        telegram_notifier.send_bundle(watchlist_reports, sector_summary, market_regime_label, run_summary=run_summary)
    elif not watchlist_reports:
        logger.info("No special activity detected. Telegram send skipped.")
    else:
        logger.info("Telegram payload unchanged. Skipping duplicate send.")

    state_manager.save_current_state(new_state)

    if watchlist_data_for_plot:
        plotting.create_comparison_chart(watchlist_data_for_plot, benchmark_data)
        logger.info("Watchlist performance charts updated and saved.")

    logger.info(f"Saved comparison snapshot to {COMPARISON_FILE}")
    logger.info("JFO Engine: Analysis Cycle Complete")
    logger.info("=" * 60)



def _extract_downloaded_ticker(batch_raw, ticker):
    if isinstance(batch_raw.columns, pd.MultiIndex):
        if ticker in batch_raw.columns.get_level_values(0):
            return batch_raw[ticker].copy().dropna(subset=["Close"])
        return pd.DataFrame()
    return batch_raw.copy().dropna(subset=["Close"])


def run_quant_research_report():
    """Generate a research-focused snapshot for the current watchlist."""
    if yf is None or calculate_metrics is None:
        raise RuntimeError("Quant reports require yfinance and pandas-ta. Install compatible dependencies before running --quant-report.")

    database.initialize_database()
    config = load_config()
    settings = config.get("settings", {})
    watchlist = [ticker.upper() for ticker in load_watchlist_data()]
    if not watchlist:
        print("Watchlist is empty. Add tickers before running --quant-report.")
        return

    period, interval, download_period = _research_download_params(settings, period_key="quant_period", default_period="2y")
    benchmark_symbol = config.get("benchmark", "SPY")
    risk_free_rate = float(settings.get("risk_free_rate", 0.045))

    batch_raw = yf.download(watchlist, period=download_period, interval=interval, group_by="ticker", threads=True, progress=False, auto_adjust=False)
    benchmark_raw = yf.download(benchmark_symbol, period=download_period, interval=interval, progress=False, auto_adjust=False)
    benchmark_data = _normalize_benchmark_data(benchmark_raw) if not benchmark_raw.empty else None
    market_payload, sector_data = _build_market_intelligence(download_period, interval)

    rows = []
    for ticker in watchlist:
        df = _extract_downloaded_ticker(batch_raw, ticker)
        if df.empty:
            rows.append({"ticker": ticker, "error": "No price data returned"})
            continue
        analyzed = calculate_metrics(df, benchmark_data, config=config) if benchmark_data is not None else df
        base_rating = scoring.generate_rating(analyzed, config=config)
        quant_payload = quant_analytics.comprehensive_stock_analysis(analyzed, benchmark_data, risk_free_rate=risk_free_rate)
        intelligence_payload = intelligence_scoring.final_stock_score(analyzed, benchmark_df=benchmark_data)
        score_date = analyzed.index[-1].date().isoformat() if hasattr(analyzed.index[-1], "date") else str(analyzed.index[-1])
        previous_scores = database.recent_scores(ticker, limit=1)
        why_payload = why_now.evaluate_why_now(ticker, analyzed, intelligence_payload, previous_scores=previous_scores, market_payload=market_payload)
        database.upsert_stock(ticker)
        database.store_price_history(ticker, df)
        database.store_technical_metrics(ticker, analyzed)
        database.store_stock_score(ticker, score_date, intelligence_payload)
        rating = _combine_rating(base_rating, intelligence_payload, why_payload)
        latest = analyzed.iloc[-1]
        row = _build_comparison_row(ticker, latest, rating, quant_payload=quant_payload, intelligence_payload=intelligence_payload, why_now_payload=why_payload)
        row["research_note"] = research_reports.stock_research_note(ticker, latest, rating, quant_payload)
        rows.append(row)

    payload = {
        "generated_at": datetime.now().isoformat(),
        "benchmark": benchmark_symbol,
        "summary": research_reports.watchlist_summary(rows),
        "tickers": rows,
    }
    with open(QUANT_RESEARCH_FILE, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"Quant research snapshot saved to {QUANT_RESEARCH_FILE}")




def _build_portfolio_context_for_discovery(period="2y", interval="1d"):
    try:
        watchlist = [ticker.upper() for ticker in load_watchlist_data()]
        portfolio_payload = portfolio_engine.load_portfolio(fallback_tickers=watchlist)
        positions = portfolio_payload.get("positions", [])
        tickers = sorted({position["ticker"] for position in positions})
        if not tickers:
            return {}
        raw = yf.download(tickers, period=period, interval=interval, group_by="ticker", threads=True, progress=False, auto_adjust=False)
        price_data = {ticker: _extract_downloaded_ticker(raw, ticker) for ticker in tickers}
        report = portfolio_engine.generate_portfolio_report(positions=positions, price_data=price_data)
        sector_map = {position["ticker"]: position.get("sector", "Unknown") for position in positions}
        return {"positions": positions, "report": report, "sector_map": sector_map}
    except Exception as exc:
        logger.warning(f"Portfolio context unavailable for stock discovery: {exc}")
        return {}


def run_stock_discovery(screener_name="quality_momentum", ticker=None):
    """Generate stock discovery rankings and individual intelligence reports."""
    if yf is None or calculate_metrics is None:
        raise RuntimeError("Stock discovery requires yfinance and indicator calculations.")

    database.initialize_database()
    os.makedirs(STOCK_REPORT_DIR, exist_ok=True)
    config = load_config()
    settings = config.get("settings", {})
    watchlist = [item.upper() for item in load_watchlist_data()]
    symbols = [ticker.upper()] if ticker else watchlist
    if not symbols:
        print("No tickers available for stock discovery.")
        return

    period, interval, download_period = _research_download_params(settings, period_key="quant_period", default_period="2y")
    benchmark_symbol = config.get("benchmark", "SPY")
    raw = yf.download(symbols, period=download_period, interval=interval, group_by="ticker", threads=True, progress=False, auto_adjust=False)
    benchmark_raw = yf.download(benchmark_symbol, period=download_period, interval=interval, progress=False, auto_adjust=False)
    benchmark_data = _normalize_benchmark_data(benchmark_raw) if not benchmark_raw.empty else None
    market_payload, sector_data = _build_market_intelligence(download_period, interval)
    portfolio_context = _build_portfolio_context_for_discovery(period=download_period, interval=interval)

    rows = []
    for symbol in symbols:
        try:
            df = _extract_downloaded_ticker(raw, symbol)
            if df.empty:
                rows.append({"ticker": symbol, "error": "No price data returned"})
                continue
            analyzed = calculate_metrics(df, benchmark_data, config=config) if benchmark_data is not None else df
            sector_df = None
            report = stock_discovery.build_stock_intelligence(
                symbol,
                analyzed,
                benchmark_data,
                market_payload,
                yf_module=yf,
                sector_df=sector_df,
                portfolio_context=portfolio_context,
            )
            database.upsert_stock(
                symbol,
                company_name=report.get("fundamentals", {}).get("company_name"),
                sector=report.get("fundamentals", {}).get("sector"),
                industry=report.get("fundamentals", {}).get("industry"),
            )
            database.store_stock_intelligence_report(symbol, datetime.now().date().isoformat(), report)
            database.store_news_events(symbol, report.get("news", {}).get("items", []))
            with open(os.path.join(STOCK_REPORT_DIR, f"{symbol}.json"), "w") as f:
                json.dump(report, f, indent=2, default=str)
            rows.append(report)
        except Exception as exc:
            logger.warning(f"Error building stock intelligence for {symbol}: {exc}")
            rows.append({"ticker": symbol, "error": str(exc)})

    discovery = stock_discovery.discover_stocks([row for row in rows if "score" in row], screener_name=screener_name)
    discovery["errors"] = [row for row in rows if "error" in row]
    with open(STOCK_DISCOVERY_FILE, "w") as f:
        json.dump(discovery, f, indent=2, default=str)
    database.store_discovery_run(datetime.now().date().isoformat(), discovery)

    if ticker:
        report = next((row for row in rows if row.get("ticker") == ticker.upper()), None)
        print(report.get("report") if report and "report" in report else json.dumps(report, indent=2, default=str))
    else:
        print(f"Stock discovery saved to {STOCK_DISCOVERY_FILE}")
        print(discovery.get("summary"))

def run_portfolio_report(send_alert=False):
    """Generate portfolio-level risk, performance, and quant intelligence."""
    if yf is None:
        raise RuntimeError("Portfolio reports require yfinance.")

    database.initialize_database()
    config = load_config()
    settings = config.get("settings", {})
    watchlist = [ticker.upper() for ticker in load_watchlist_data()]
    portfolio_payload = portfolio_engine.load_portfolio(fallback_tickers=watchlist)
    positions = portfolio_payload.get("positions", [])
    tickers = sorted({position["ticker"] for position in positions})
    if not tickers:
        print("No portfolio positions found. Add portfolio.json or watchlist tickers first.")
        return

    period = settings.get("portfolio_period", "2y")
    interval = "1d"
    raw = yf.download(tickers, period=period, interval=interval, group_by="ticker", threads=True, progress=False, auto_adjust=False)
    price_data = {ticker: _extract_downloaded_ticker(raw, ticker) for ticker in tickers}

    factor_symbols = {
        "Market Factor": "SPY",
        "Technology Factor": "XLK",
        "Growth Factor": "QQQ",
        "Value Factor": "IWD",
        "Momentum Factor": "MTUM",
        "Low Volatility Factor": "SPLV",
        "Interest Rate Sensitivity": "TLT",
    }
    factor_data = _download_context(factor_symbols, period, interval)
    benchmark_df = factor_data.get("Market Factor")
    risk_free_rate = float(settings.get("risk_free_rate", 0.045))

    report = portfolio_engine.generate_portfolio_report(
        positions=positions,
        price_data=price_data,
        benchmark_df=benchmark_df,
        factor_prices=factor_data,
        risk_free_rate=risk_free_rate,
    )
    with open(PORTFOLIO_REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2, default=str)
    database.store_portfolio_snapshot(datetime.now().date().isoformat(), report)
    print(f"Portfolio intelligence report saved to {PORTFOLIO_REPORT_FILE}")

    why_payload = report.get("why_now", {})
    if send_alert and why_payload.get("send_alert"):
        message = _format_portfolio_telegram(report)
        if should_send_report(message):
            telegram_notifier.send_long_message(message)
    elif send_alert:
        print("No portfolio Why Now trigger. Telegram portfolio alert skipped.")


def _format_portfolio_telegram(report):
    health = report.get("portfolio_health", {})
    why_payload = report.get("why_now", {})
    risk_contrib = report.get("risk_contributions", {})
    top_risk = sorted(risk_contrib.items(), key=lambda item: item[1], reverse=True)[:3]
    top_risk_text = ", ".join(f"{ticker}: {value:.1f}%" for ticker, value in top_risk) or "N/A"
    return "\n".join(
        [
            "PORTFOLIO RISK ALERT",
            "",
            f"Portfolio Health Score: {health.get('score', 'N/A')}/100 ({health.get('classification', 'N/A')})",
            f"Why Now: {why_payload.get('reason', 'N/A')}",
            f"Impact: {why_payload.get('evidence', 'N/A')}",
            f"Annual Volatility: {report.get('variance', {}).get('annual_volatility', 0) * 100:.2f}%",
            f"Sharpe Ratio: {report.get('sharpe', {}).get('sharpe_ratio', 0):.2f}",
            f"Average Correlation: {report.get('correlation', {}).get('average_correlation', 0):.2f}",
            f"Top Risk Contributors: {top_risk_text}",
            f"What To Watch: {why_payload.get('what_to_watch', 'Monitor volatility, correlation, drawdown, and sector concentration.')}",
            "",
            "Educational risk analysis only, not financial advice.",
        ]
    )

def run_option_lab(args):
    """Run the standalone options analytics lab from CLI inputs."""
    if args.stock_price is None or args.strike is None:
        raise SystemExit("--option-lab requires --stock-price and --strike")
    payload = options_analytics.option_lab_report(
        stock_price=args.stock_price,
        strike=args.strike,
        days_to_expiry=args.days,
        risk_free_rate=args.rate,
        volatility=args.volatility,
        market_price=args.market_price,
        option_type=args.option_type,
    )
    payload["greek_explanations"] = options_analytics.greek_explanations(payload.get("greeks", {}))
    print(json.dumps(payload, indent=2))

def main():
    parser = argparse.ArgumentParser(description="Jain Family Office: Stock Intelligence System")
    parser.add_argument("--add", nargs="+", help="Add specific tickers to the watchlist")
    parser.add_argument("--remove", nargs="+", help="Remove specific tickers from the watchlist")
    parser.add_argument("--list", action="store_true", help="Display the current active watchlist")
    parser.add_argument("--analyze", action="store_true", help="Manually trigger the full analysis engine")
    parser.add_argument("--force", action="store_true", help="Run analysis even when market-hours checks are enabled")
    parser.add_argument("--quant-report", action="store_true", help="Generate a watchlist quant research snapshot")
    parser.add_argument("--option-lab", action="store_true", help="Run Black-Scholes, Greeks, IV, break-even, and Monte Carlo analysis")
    parser.add_argument("--dashboard", action="store_true", help="Generate the static quant research dashboard HTML")
    parser.add_argument("--init-db", action="store_true", help="Create or migrate the SQLite research database")
    parser.add_argument("--signal-performance", action="store_true", help="Print historical signal validation summary")
    parser.add_argument("--portfolio-report", action="store_true", help="Generate portfolio risk, performance, and intelligence report")
    parser.add_argument("--stock-discovery", action="store_true", help="Run stock discovery and screening intelligence")
    parser.add_argument("--screener", default="quality_momentum", help="Saved screener name for stock discovery")
    parser.add_argument("--stock-report", help="Generate an individual stock intelligence report for one ticker")
    parser.add_argument("--send-portfolio-alert", action="store_true", help="Send portfolio Telegram alert when a Why Now trigger exists")
    parser.add_argument("--stock-price", type=float, help="Option lab underlying stock price")
    parser.add_argument("--strike", type=float, help="Option lab strike price")
    parser.add_argument("--days", type=float, default=30, help="Option lab days to expiration")
    parser.add_argument("--rate", type=float, default=0.045, help="Option lab annual risk-free rate")
    parser.add_argument("--volatility", type=float, default=0.30, help="Option lab annualized volatility")
    parser.add_argument("--market-price", type=float, help="Option lab observed market option price")
    parser.add_argument("--option-type", choices=["call", "put"], default="call", help="Option lab contract type")

    args = parser.parse_args()

    if args.add or args.remove:
        manage_cli_updates(args.add, args.remove)

    if args.list:
        current_watchlist = load_watchlist_data()
        print(f"Current Active Watchlist: {', '.join(current_watchlist)}")

    if args.init_db:
        database.initialize_database()
        print("SQLite research database initialized at state/jfo_quant.db")

    if args.signal_performance:
        print(json.dumps(signal_validation.summarize_signal_performance(), indent=2))

    if args.quant_report:
        run_quant_research_report()

    if args.option_lab:
        run_option_lab(args)

    if args.portfolio_report:
        run_portfolio_report(send_alert=args.send_portfolio_alert)

    if args.stock_discovery:
        run_stock_discovery(screener_name=args.screener)

    if args.stock_report:
        run_stock_discovery(screener_name=args.screener, ticker=args.stock_report)

    if args.dashboard:
        path = quant_dashboard.generate_dashboard()
        print(f"Quant dashboard saved to {path}")

    if args.analyze or args.force:
        run_analytics_engine(force=True)
    elif not any(vars(args).values()):
        run_analytics_engine(force=False)


if __name__ == "__main__":
    main()
