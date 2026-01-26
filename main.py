import os
import json
import argparse
import sys
import requests
import hashlib
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import yfinance as yf
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import your custom logic modules
import indicators
import state_manager
import telegram_notifier 
import plotting 
import scoring
from logger_config import setup_logger
from utils import retry_on_failure, cache_result, safe_request, validate_ticker

# Initialize logger with environment variable support
log_level = os.getenv('LOG_LEVEL', 'INFO')
logger = setup_logger(log_level=log_level) 

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================
WATCHLIST_FILE = 'watchlist.json'
STATE_DIR = 'state'
HASH_FILE = os.path.join(STATE_DIR, 'last_report_hash.json')

# CRITICAL: Create folders before any analysis starts
os.makedirs('plots', exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs('logs', exist_ok=True)
os.makedirs('cache', exist_ok=True)

# ==========================================
# PART 1: WATCHLIST MANAGEMENT
# ==========================================

def load_watchlist_data():
    """Loads tickers from the JSON file. Creates file if missing."""
    if not os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE, 'w') as f:
            json.dump(["SPY", "QQQ"], f) # Default starters
        return ["SPY", "QQQ"]
    
    with open(WATCHLIST_FILE, 'r') as f:
        try:
            data = json.load(f)
            return data if isinstance(data, list) else []
        except json.JSONDecodeError:
            return []

def save_watchlist_data(tickers):
    """Saves the unique list of tickers back to the JSON file."""
    unique_tickers = sorted(list(set(t.upper() for t in tickers)))
    with open(WATCHLIST_FILE, 'w') as f:
        json.dump(unique_tickers, f, indent=4)

def manage_cli_updates(add_list=None, remove_list=None):
    """Handles the Add/Remove logic from Command Line Arguments."""
    current_tickers = load_watchlist_data()
    updated = False

    if add_list:
        for t in add_list:
            t_upper = t.upper()
            if t_upper not in current_tickers:
                current_tickers.append(t_upper)
                print(f"Added: {t_upper}")
                updated = True
            else:
                print(f"Skipped (Already exists): {t_upper}")

    if remove_list:
        for t in remove_list:
            t_upper = t.upper()
            if t_upper in current_tickers:
                current_tickers.remove(t_upper)
                print(f"Removed: {t_upper}")
                updated = True
            else:
                print(f"Skipped (Not in list): {t_upper}")

    if updated:
        save_watchlist_data(current_tickers)
        print(f"Watchlist updated. Total tickers: {len(current_tickers)}")
    else:
        print("No changes made to the watchlist.")

# ==========================================
# PART 2: UTILITIES & DEDUPLICATION
# ==========================================

def load_config():
    """Loads settings from config/config.json."""
    config_path = os.path.join('config', 'config.json')
    try:
        if not os.path.exists(config_path):
             return {"benchmark": "SPY"} # Default if config missing
        with open(config_path, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Configuration Error: {e}")
        return {"benchmark": "SPY"}

def is_market_open():
    """Gatekeeper: Checks if NYSE is currently open."""
    nyse = mcal.get_calendar('NYSE')
    now_utc = datetime.now(pytz.utc)
    
    schedule = nyse.schedule(start_date=now_utc, end_date=now_utc)
    if schedule.empty:
        return False
        
    market_open = schedule.iloc[0].market_open
    market_close = schedule.iloc[0].market_close
    
    return market_open <= now_utc <= market_close

def should_send_report(content_to_hash):
    """Checks if the report content is identical to the last one sent."""
    current_hash = hashlib.md5(content_to_hash.encode('utf-8')).hexdigest()
    
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, 'r') as f:
            try:
                data = json.load(f)
                last_hash = data.get('hash')
                if last_hash == current_hash:
                    return False
            except:
                pass

    # Save new hash for the next comparison
    with open(HASH_FILE, 'w') as f:
        json.dump({'hash': current_hash, 'timestamp': str(datetime.now())}, f)
    return True

@cache_result(cache_key="sp500_sectors", ttl_seconds=86400)  # Cache for 24 hours
@retry_on_failure(max_retries=3, delay=2.0)
def get_sp500_sectors() -> Dict[str, str]:
    """
    Scrapes S&P 500 list from Wikipedia for sector-based scanning.
    Results are cached for 24 hours to reduce API calls.
    """
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=30)
        table = pd.read_html(response.text)[0]
        table['Symbol'] = table['Symbol'].str.replace('.', '-', regex=False)
        sector_map = dict(zip(table['Symbol'], table['GICS Sector']))
        logger.info(f"Successfully fetched {len(sector_map)} S&P 500 tickers")
        return sector_map
    except Exception as e:
        logger.error(f"Error fetching S&P 500 sector list: {e}", exc_info=True)
        # Fallback to avoid crash
        return {"AAPL": "Information Technology", "MSFT": "Information Technology", "NVDA": "Information Technology"}

# ==========================================
# PART 3: MAIN ANALYTICS ENGINE
# ==========================================

def run_analytics_engine():
    """Main analytics engine orchestrator."""
    logger.info("=" * 60)
    logger.info("Jain Family Office: Market Intelligence Engine v2")
    logger.info("=" * 60)
    
    # 1. MARKET GATEKEEPER
    if not is_market_open():
        logger.info("Market is currently closed. Using last available close data.")

    # 2. INITIALIZATION
    config = load_config()
    if not config:
        logger.error("Failed to load configuration. Exiting.")
        return
    
    watchlist = load_watchlist_data()
    logger.info(f"Watchlist loaded: {len(watchlist)} tickers")
    
    benchmark_symbol = config.get("benchmark", "SPY")
    sector_map = get_sp500_sectors()
    all_sp500_tickers = list(sector_map.keys())
    logger.info(f"S&P 500 sector map loaded: {len(all_sp500_tickers)} tickers")
    
    manual_input = os.getenv('MANUAL_TICKERS', '')
    if manual_input:
        full_scan_list = [t.strip().upper() for t in manual_input.split(',')]
        # Validate tickers
        full_scan_list = [t for t in full_scan_list if validate_ticker(t)]
        logger.info(f"Manual override: Scanning {len(full_scan_list)} tickers: {full_scan_list}")
    else:
        full_scan_list = list(set(watchlist + all_sp500_tickers))
        logger.info(f"Full scan list: {len(full_scan_list)} unique tickers")

    # 3. DATA ACQUISITION
    logger.info(f"[1/4] Batch downloading data for {len(full_scan_list)} tickers...")
    try:
        batch_data = yf.download(full_scan_list, period="1y", interval="1d", group_by='ticker', threads=True, progress=False)
        logger.info("Batch data download completed")
        
        benchmark_data = yf.download(benchmark_symbol, period="1y", progress=False)
        logger.info(f"Benchmark data ({benchmark_symbol}) downloaded")
    except Exception as e:
        logger.error(f"Data download failed: {e}", exc_info=True)
        return

    if benchmark_data.empty:
        logger.critical("Benchmark data missing. Cannot proceed with analysis.")
        return

    regime_label = indicators.get_market_regime_label(benchmark_data)

    # 4. PROCESSING LOGIC
    logger.info("[2/4] Analyzing Leaders and Drops...")
    prev_state = state_manager.load_previous_state()
    current_full_state = {} 
    
    watchlist_data_for_plot = {}
    watchlist_reports = []
    leader_reports = {} 
    laggard_reports = {} # Stores Market Drops (Weakness)
    
    processed_count = 0
    skipped_count = 0

    for ticker in full_scan_list:
        try:
            # Handle multi-index data from batch download
            if len(full_scan_list) > 1:
                if ticker not in batch_data.columns.levels[0]:
                    continue
                stock_data = batch_data[ticker].dropna()
            else:
                stock_data = batch_data.dropna()

            if stock_data.empty or len(stock_data) < 50:
                skipped_count += 1
                logger.debug(f"Skipping {ticker}: insufficient data ({len(stock_data) if not stock_data.empty else 0} rows)")
                continue

            # TA, Scoring, and Alerts
            analyzed_data = indicators.calculate_metrics(stock_data, benchmark_data)
            rating_result = scoring.generate_rating(analyzed_data)
            ticker_alerts = state_manager.get_ticker_alerts(ticker, analyzed_data, prev_state)
            current_full_state = state_manager.update_ticker_state(ticker, analyzed_data, current_full_state)

            report_line = telegram_notifier.format_ticker_report(ticker, ticker_alerts, analyzed_data.iloc[-1], rating_result)
            sector = sector_map.get(ticker, "Other Sectors")

            # Sorting into Groups
            if ticker in watchlist:
                watchlist_reports.append(report_line)
                watchlist_data_for_plot[ticker] = analyzed_data
            
            # S&P 500 Scanning Logic
            if rating_result['score'] >= 85:
                if sector not in leader_reports: leader_reports[sector] = []
                leader_reports[sector].append(report_line)
            elif rating_result['score'] <= 25:
                if sector not in laggard_reports: laggard_reports[sector] = []
                laggard_reports[sector].append(report_line)
            
            processed_count += 1
            if processed_count % 50 == 0:
                logger.debug(f"Processed {processed_count}/{len(full_scan_list)} tickers...")

        except Exception as e:
            skipped_count += 1
            logger.warning(f"Error processing {ticker}: {e}", exc_info=True)
            continue
    
    logger.info(f"Processing complete: {processed_count} processed, {skipped_count} skipped")

    # 5. DEDUPLICATION & NOTIFICATION
    logger.info("[3/4] Compiling Executive Report...")
    
    # Section A: Watchlist
    watchlist_segment = "📌 **PRIMARY WATCHLIST**\n" + ("".join(watchlist_reports) if watchlist_reports else "_No active data._\n")
    
    # Section B: Leaders
    leader_segment = "\n📈 **S&P 500 MOMENTUM LEADERS**\n"
    if not leader_reports:
        leader_segment += "_No Tier 1 Leaders found today._"
    else:
        for sec in sorted(leader_reports.keys()):
            leader_segment += f"\n📂 *{sec}*\n" + "".join(leader_reports[sec][:3])

    # Section C: Drops (New)
    drop_segment = "\n📉 **S&P 500 MARKET DROPS (WEAKNESS)**\n"
    if not laggard_reports:
        drop_segment += "_No significant drops found today._"
    else:
        for sec in sorted(laggard_reports.keys()):
            drop_segment += f"\n📂 *{sec}*\n" + "".join(laggard_reports[sec][:3])

    # Combine all text to fingerprint the run
    full_report_body = watchlist_segment + leader_segment + drop_segment
    
    if should_send_report(full_report_body):
        telegram_notifier.send_bundle([watchlist_segment, leader_segment, drop_segment], regime_label)
        logger.info("New technical events detected. Notification dispatched.")
    else:
        logger.info("Analysis complete. Data identical to last run; silence maintained.")

    # Save finalized state for alert tracking
    state_manager.save_current_state(current_full_state)
    
    # 6. VISUALIZATION
    if watchlist_data_for_plot:
        logger.info("[4/4] Generating Dashboards...")
    
        try:
            plotting.create_comparison_chart(watchlist_data_for_plot, benchmark_data)
            logger.info(f"Generated charts for {len(watchlist_data_for_plot)} tickers")
        except Exception as e:
            logger.error(f"Plotting error: {e}", exc_info=True)
    else:
        print("[4/4] SKIP: No data found in watchlist_data_for_plot. Check your tickers!")

    logger.info("JFO Engine: Cycle Complete.")
    logger.info("=" * 60)

# ==========================================
# ENTRY POINT
# ==========================================

def main():
    parser = argparse.ArgumentParser(description="JFO Market Intelligence Engine")
    parser.add_argument('--add', nargs='+', help="Add tickers to watchlist")
    parser.add_argument('--remove', nargs='+', help="Remove tickers from watchlist")
    parser.add_argument('--list', action='store_true', help="List current watchlist")
    parser.add_argument('--analyze', action='store_true', help="Run the full engine")

    args = parser.parse_args()

    if args.add or args.remove:
        manage_cli_updates(add_list=args.add, remove_list=args.remove)
    
    if args.list:
        wl = load_watchlist_data()
        print(f"Current Watchlist ({len(wl)}): {', '.join(wl)}")

    # Default to analyzing if no management args provided
    if args.analyze or not any(vars(args).values()):
        run_analytics_engine()

if __name__ == "__main__":
    main()
