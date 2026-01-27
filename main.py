import os
import json
import argparse
import hashlib
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import yfinance as yf
import requests
from io import StringIO
from datetime import datetime
from typing import Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project imports
from indicators import calculate_metrics, get_market_regime_label
import state_manager
import telegram_notifier
import plotting
import scoring
from logger_config import setup_logger
from utils import retry_on_failure, cache_result, read_html_table

# Logger initialization
log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(log_level=log_level)

# ==========================================
# CONFIGURATION
# ==========================================
WATCHLIST_FILE = "watchlist.json"
STATE_DIR = "state"
HASH_FILE = os.path.join(STATE_DIR, "last_report_hash.json")

# Ensure environment directories exist
os.makedirs("plots", exist_ok=True)
os.makedirs("logs", exist_ok=True)
os.makedirs("cache", exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

# ==========================================
# WATCHLIST MANAGEMENT
# ==========================================
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
        for t in add_list:
            t = t.upper()
            if t not in current:
                current.append(t)
                changed = True
    if remove_list:
        for t in remove_list:
            t = t.upper()
            if t in current:
                current.remove(t)
                changed = True
    if changed:
        save_watchlist_data(current)

# ==========================================
# UTILITIES
# ==========================================
def load_config():
    path = os.path.join("config", "config.json")
    if not os.path.exists(path):
        return {"benchmark": "SPY"}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {"benchmark": "SPY"}

def is_market_open():
    nyse = mcal.get_calendar("NYSE")
    now = datetime.now(pytz.utc)
    sched = nyse.schedule(start_date=now, end_date=now)
    if sched.empty:
        return False
    return sched.iloc[0].market_open <= now <= sched.iloc[0].market_close

def should_send_report(content):
    content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, "r") as f:
            try:
                if json.load(f).get("hash") == content_hash:
                    return False
            except Exception:
                pass
    with open(HASH_FILE, "w") as f:
        json.dump({"hash": content_hash, "ts": str(datetime.now())}, f)
    return True

# ==========================================
# S&P 500 SECTOR SCRAPER
# ==========================================
@cache_result(cache_key="sp500_sectors", ttl_seconds=86400)
@retry_on_failure(max_retries=3, delay=2)
def get_sp500_sectors() -> Dict[str, str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    response = requests.get(url, headers=headers, timeout=30)
    
    # Use read_html via StringIO to handle Wikipedia's table structure
    tables = pd.read_html(StringIO(response.text))
    table = tables[0]
    
    table["Symbol"] = table["Symbol"].str.replace(".", "-", regex=False)
    return dict(zip(table["Symbol"], table["GICS Sector"]))

# ==========================================
# MAIN ANALYTICS ENGINE
# ==========================================
def run_analytics_engine():
    logger.info("=" * 60)
    logger.info("Jain Family Office: Market Intelligence Engine v2")
    logger.info("=" * 60)

    if not is_market_open():
        logger.info("Market closed – using last available close data")

    config = load_config()
    watchlist = load_watchlist_data()
    benchmark_symbol = config.get("benchmark", "SPY")
    
    sector_map = get_sp500_sectors()
    scan_list = list(set(watchlist + list(sector_map.keys())))
    logger.info(f"Scanning {len(scan_list)} total tickers")

    # 1. Download Data
    batch_raw = yf.download(
        scan_list,
        period="1y",
        interval="1d",
        group_by="ticker",
        threads=True,
        progress=False,
    )
    
    benchmark_raw = yf.download(
        benchmark_symbol,
        period="1y",
        progress=False,
    )

    if benchmark_raw.empty:
        logger.critical("Failed to download benchmark data. Aborting.")
        return

    # Normalize Benchmark (handle MultiIndex if yf returns it)
    if isinstance(benchmark_raw.columns, pd.MultiIndex):
        benchmark_data = benchmark_raw.xs("Close", level=1, axis=1) if "Close" in benchmark_raw.columns.get_level_values(1) else benchmark_raw.iloc[:, 0]
    else:
        benchmark_data = benchmark_raw["Close"]
    
    benchmark_data = benchmark_data.to_frame("Close").dropna()
    market_regime = get_market_regime_label(benchmark_data)
    logger.info(f"Detected Market Regime: {market_regime}")

    # 2. Process Tickers
    prev_state = state_manager.load_previous_state()
    new_state = {}
    watchlist_data_for_plot = {}
    watchlist_reports = []
    leaders = {}
    laggards = {}

    for ticker in scan_list:
        try:
            # Extract and flatten data for this ticker
            if ticker not in batch_raw.columns.levels[0]:
                continue
            
            df = batch_raw[ticker].copy().dropna(subset=["Close"])
            if len(df) < 60:
                continue

            # Run Analysis
            analyzed = calculate_metrics(df, benchmark_data)
            rating = scoring.generate_rating(analyzed)
            alerts = state_manager.get_ticker_alerts(ticker, analyzed, prev_state)
            new_state = state_manager.update_ticker_state(ticker, analyzed, new_state)
            
            line = telegram_notifier.format_ticker_report(ticker, alerts, analyzed.iloc[-1], rating)
            sector = sector_map.get(ticker, "Other")

            # Grouping for report
            if ticker in watchlist:
                watchlist_reports.append(line)
                watchlist_data_for_plot[ticker] = analyzed
            
            if rating["score"] >= 85:
                leaders.setdefault(sector, []).append(line)
            elif rating["score"] <= 25:
                laggards.setdefault(sector, []).append(line)

        except Exception as e:
            logger.warning(f"Failed to process {ticker}: {e}")

    # 3. Report Compilation
    report_body = "📌 **WATCHLIST UPDATES**\n" + ("".join(watchlist_reports) if watchlist_reports else "No significant changes.\n")
    
    if leaders:
        report_body += "\n📈 **TOP MOMENTUM LEADERS**\n"
        for sec, items in sorted(leaders.items()):
            report_body += f"📂 *{sec}*: " + ", ".join([i.split()[0] for i in items[:5]]) + "\n"

    if laggards:
        report_body += "\n📉 **MARKET LAGGARDS (WATCH FOR DROPS)**\n"
        for sec, items in sorted(laggards.items()):
            report_body += f"📂 *{sec}*: " + ", ".join([i.split()[0] for i in items[:5]]) + "\n"

    # 4. Finalizing
    if should_send_report(report_body):
        telegram_notifier.send_bundle([report_body], market_regime)
        logger.info("Executive report dispatched to Telegram")
    
    state_manager.save_current_state(new_state)

    if watchlist_data_for_plot:
        plotting.create_comparison_chart(watchlist_data_for_plot, benchmark_data)
        logger.info("Watchlist comparison charts updated")

    logger.info("JFO Engine: Cycle Complete")
    logger.info("=" * 60)

# ==========================================
# ENTRY POINT
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="JFO Stock Intelligence System")
    parser.add_argument("--add", nargs="+", help="Add tickers to watchlist")
    parser.add_argument("--remove", nargs="+", help="Remove tickers from watchlist")
    parser.add_argument("--list", action="store_true", help="List current watchlist")
    parser.add_argument("--analyze", action="store_true", help="Run the full analysis engine")
    
    args = parser.parse_args()

    if args.add or args.remove:
        manage_cli_updates(args.add, args.remove)
    
    if args.list:
        wl = load_watchlist_data()
        print(f"Current Watchlist: {', '.join(wl)}")
    
    # Default to analysis if no flags or specifically requested
    if args.analyze or not any(vars(args).values()):
        run_analytics_engine()

if __name__ == "__main__":
    main()
