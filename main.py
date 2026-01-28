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
            if isinstance(data, list):
                return data
            else:
                return []
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
                logger.info(f"Added {t} to watchlist")
                
    if remove_list:
        for t in remove_list:
            t = t.upper()
            if t in current:
                current.remove(t)
                changed = True
                logger.info(f"Removed {t} from watchlist")
                
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
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return {"benchmark": "SPY"}

def is_market_open():
    """Checks the NYSE calendar to see if the market is currently active."""
    nyse = mcal.get_calendar("NYSE")
    now = datetime.now(pytz.utc)
    
    # Get the schedule for today
    sched = nyse.schedule(start_date=now, end_date=now)
    
    if sched.empty:
        return False
        
    # Schedule times are typically returned in UTC
    market_open = sched.iloc[0].market_open
    market_close = sched.iloc[0].market_close
    
    return market_open <= now <= market_close

def should_send_report(content):
    """Prevents duplicate reports by hashing the content."""
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
        json.dump({
            "hash": content_hash, 
            "timestamp": str(datetime.now())
        }, f)
        
    return True

# ==========================================
# S&P 500 SECTOR SCRAPER
# ==========================================
@cache_result(cache_key="sp500_sectors", ttl_seconds=86400)
@retry_on_failure(max_retries=3, delay=2)
def get_sp500_sectors() -> Dict[str, str]:
    """Scrapes Wikipedia for S&P 500 company sector data."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    
    # Using read_html to handle the table parsing
    tables = pd.read_html(StringIO(response.text))
    table = tables[0]
    
    # Clean ticker symbols for yfinance (replace dots with dashes)
    table["Symbol"] = table["Symbol"].str.replace(".", "-", regex=False)
    
    return dict(zip(table["Symbol"], table["GICS Sector"]))

# ==========================================
# MAIN ANALYTICS ENGINE
# ==========================================
def run_analytics_engine():
    logger.info("=" * 60)
    logger.info("Jain Family Office: Market Intelligence Engine v2")
    logger.info("=" * 60)

    # 🛑 THE FIX: Stop everything immediately if the market is closed.
    if not is_market_open():
        logger.info("MARKET IS CLOSED. Cycle terminated to avoid duplicate notifications.")
        return

    config = load_config()
    watchlist = load_watchlist_data()
    benchmark_symbol = config.get("benchmark", "SPY")
    
    sector_map = get_sp500_sectors()
    scan_list = list(set(watchlist + list(sector_map.keys())))
    
    logger.info(f"Scanning {len(scan_list)} total tickers (Watchlist + S&P 500)")

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
        logger.critical("CRITICAL: Failed to download benchmark data. Aborting cycle.")
        return

    # Normalize Benchmark Data
    if isinstance(benchmark_raw.columns, pd.MultiIndex):
        benchmark_data = benchmark_raw.xs("Close", level=1, axis=1) if "Close" in benchmark_raw.columns.get_level_values(1) else benchmark_raw.iloc[:, 0]
    else:
        benchmark_data = benchmark_raw["Close"]
    
    benchmark_data = benchmark_data.to_frame("Close").dropna()
    
    # Detect Market Regime
    market_regime = get_market_regime_label(benchmark_data)
    logger.info(f"Detected Market Regime: {market_regime}")

    # 2. Process Tickers
    prev_state = state_manager.load_previous_state()
    new_state = {}
    watchlist_data_for_plot = {}
    watchlist_reports = []
    
    # Storage for Sector Performance and Leaders
    sector_performance_tracker = {}
    leaders = {}
    laggards = {}

    for ticker in scan_list:
        try:
            # Ensure ticker data exists in batch
            if ticker not in batch_raw.columns.levels[0]:
                continue
            
            df = batch_raw[ticker].copy().dropna(subset=["Close"])
            if len(df) < 60:
                continue

            # Core Calculations
            analyzed = calculate_metrics(df, benchmark_data)
            rating = scoring.generate_rating(analyzed)
            alerts = state_manager.get_ticker_alerts(ticker, analyzed, prev_state)
            
            # Update internal state
            new_state = state_manager.update_ticker_state(ticker, analyzed, new_state)
            
            # Daily Percentage Change for Sector Weighting
            daily_change = ((df['Close'].iloc[-1] - df['Close'].iloc[-2]) / df['Close'].iloc[-2]) * 100
            
            # Generate Report Snippet
            report_line = telegram_notifier.format_ticker_report(ticker, alerts, analyzed.iloc[-1], rating)
            
            # Categorize Sector
            sector = sector_map.get(ticker, "Other")
            
            if sector not in sector_performance_tracker:
                sector_performance_tracker[sector] = []
                
            sector_performance_tracker[sector].append(daily_change)

            # Handle Watchlist
            if ticker in watchlist:
                watchlist_reports.append(report_line)
                watchlist_data_for_plot[ticker] = analyzed
            
            # Identify Leaders and Laggards for Summary
            if rating["score"] >= 85:
                if sector not in leaders:
                    leaders[sector] = []
                leaders[sector].append((ticker, rating["score"]))
                
            elif rating["score"] <= 25:
                if sector not in laggards:
                    laggards[sector] = []
                laggards[sector].append((ticker, rating["score"]))

        except Exception as e:
            logger.warning(f"Error processing ticker {ticker}: {e}")

    # 3. Finalize the Sector Summary (11 Sectors)
    final_sector_stats = {}
    for sector_name, changes in sector_performance_tracker.items():
        if not changes:
            continue
            
        avg_change = sum(changes) / len(changes)
        
        # Get Top/Bottom Stock for this Sector
        all_sector_tickers = [(t, s['score']) for t, s in [(t, scoring.generate_rating(calculate_metrics(batch_raw[t].dropna(), benchmark_data))) for t in scan_list if sector_map.get(t) == sector_name]]
        
        if all_sector_tickers:
            all_sector_tickers.sort(key=lambda x: x[1])
            top_stock = all_sector_tickers[-1][0]
            bottom_stock = all_sector_tickers[0][0]
        else:
            top_stock = "N/A"
            bottom_stock = "N/A"
            
        final_sector_stats[sector_name] = {
            'change': avg_change,
            'top': top_stock,
            'bottom': bottom_stock
        }

    # 4. Dispatch to Telegram Notifier
    # The notifier handles the internal compilation and sending
    telegram_notifier.send_bundle(watchlist_reports, final_sector_stats, market_regime)

    # 5. Save State and Generate Plots
    state_manager.save_current_state(new_state)

    if watchlist_data_for_plot:
        plotting.create_comparison_chart(watchlist_data_for_plot, benchmark_data)
        logger.info("Watchlist performance charts updated and saved.")

    logger.info("JFO Engine: Analysis Cycle Complete")
    logger.info("=" * 60)

# ==========================================
# ENTRY POINT
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="Jain Family Office: Stock Intelligence System")
    parser.add_argument("--add", nargs="+", help="Add specific tickers to the watchlist")
    parser.add_argument("--remove", nargs="+", help="Remove specific tickers from the watchlist")
    parser.add_argument("--list", action="store_true", help="Display the current active watchlist")
    parser.add_argument("--analyze", action="store_true", help="Manually trigger the full analysis engine")
    
    args = parser.parse_args()

    if args.add or args.remove:
        manage_cli_updates(args.add, args.remove)
    
    if args.list:
        current_watchlist = load_watchlist_data()
        print(f"Current Active Watchlist: {', '.join(current_watchlist)}")
    
    # Run the engine if specifically requested or if no other flags are set
    if args.analyze or not any(vars(args).values()):
        run_analytics_engine()

if __name__ == "__main__":
    main()
