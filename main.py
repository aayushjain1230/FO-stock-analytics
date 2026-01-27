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
    
    # Dictionaries to hold (Ticker, Score) tuples for clean formatting
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
            
            # Store Ticker and Score cleanly for the final report
            if rating["score"] >= 85:
                leaders.setdefault(sector, []).append((ticker, rating["score"]))
            elif rating["score"] <= 25:
                laggards.setdefault(sector, []).append((ticker, rating["score"]))

        except Exception as e:
            logger.warning(f"Failed to process {ticker}: {e}")

    # 3. Report Compilation (Clean Sector Grouping)
    report_lines = []
    report_lines.append(f"🏛 **JFO MARKET INTELLIGENCE**")
    report_lines.append(f"Regime: {market_regime}")
    report_lines.append("="*20)

    # Watchlist Section (Always included if present)
    if watchlist_reports:
        report_lines.append("\n📌 **WATCHLIST UPDATES**")
        report_lines.append("".join(watchlist_reports))

    # Leaders Section
    if leaders:
        report_lines.append("\n🚀 **SECTOR LEADERS (Score 85+)**")
        for sec, items in sorted(leaders.items()):
            # Sort by score descending
            items.sort(key=lambda x: x[1], reverse=True)
            # Format: AAPL(99), MSFT(95)...
            formatted_tickers = [f"{t}({s})" for t, s in items[:6]]
            report_lines.append(f"📂 *{sec}*: {', '.join(formatted_tickers)}")

    # Laggards Section
    if laggards:
        report_lines.append("\n📉 **SECTOR LAGGARDS (Score <25)**")
        for sec, items in sorted(laggards.items()):
            # Sort by score ascending (worst first)
            items.sort(key=lambda x: x[1])
            formatted_tickers = [f"{t}({s})" for t, s in items[:6]]
            report_lines.append(f"📂 *{sec}*: {', '.join(formatted_tickers)}")

    final_report = "\n".join(report_lines)

    # 4. Finalizing
    if should_send_report(final_report):
        # We manually call the bot API here to ensure we send ONE message with NO links
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if bot_token and chat_id:
            try:
                # disable_web_page_preview=True prevents the big link previews
                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                payload = {
                    "chat_id": chat_id, 
                    "text": final_report, 
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True 
                }
                requests.post(url, json=payload)
                logger.info("Executive report dispatched to Telegram")
            except Exception as e:
                logger.error(f"Telegram Send Failed: {e}")
        else:
            logger.warning("Telegram credentials missing.")
    else:
        logger.info("No actionable changes. Telegram message skipped.")
    
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
