import os
import json
import argparse
import sys
import requests
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import yfinance as yf
from datetime import datetime

# Import your custom logic modules
import indicators
import state_manager
import telegram_notifier 
import plotting 
import scoring 

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================
WATCHLIST_FILE = 'watchlist.json'

# ==========================================
# PART 1: WATCHLIST MANAGEMENT (New Logic)
# ==========================================

def load_watchlist_data():
    """Loads tickers from the JSON file. Creates file if missing."""
    if not os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE, 'w') as f:
            json.dump([], f)
        return []
    
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
# PART 2: EXISTING ANALYTICS UTILITIES
# ==========================================

def load_config():
    """Loads settings from config/config.json."""
    config_path = os.path.join('config', 'config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Configuration Error: {e}")
        return None

def is_market_open():
    """Gatekeeper: Checks if NYSE is currently open using real-world holiday calendars."""
    nyse = mcal.get_calendar('NYSE')
    now_utc = datetime.now(pytz.utc)
    
    schedule = nyse.schedule(start_date=now_utc, end_date=now_utc)
    if schedule.empty:
        return False
        
    market_open = schedule.iloc[0].market_open
    market_close = schedule.iloc[0].market_close
    
    return market_open <= now_utc <= market_close

def get_sp500_sectors():
    try:
        # We must set a User-Agent header to avoid the 403 Forbidden error
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        
        response = requests.get(url, headers=headers)
        table = pd.read_html(response.text)[0]
        
        table['Symbol'] = table['Symbol'].str.replace('.', '-', regex=False)
        return dict(zip(table['Symbol'], table['GICS Sector']))
    except Exception as e:
        print(f"Error fetching S&P 500 sector list: {e}")
        return {}

# ==========================================
# PART 3: MAIN ANALYTICS ENGINE
# ==========================================

def run_analytics_engine():
    print("\n--- Jain Family Office: Market Intelligence Engine v2 ---")
    
    # 1. MARKET GATEKEEPER
    # Checks if NYSE is open. (Comment out the 'return' for weekend testing)
    if not is_market_open():
        print("Market is closed. Script terminating to save compute.")
        # return  # <--- Uncomment this for production to prevent running when market is closed

    # 2. INITIALIZATION
    config = load_config()
    if not config: return
    
    # UPDATED: Load watchlist from the JSON file logic instead of config.json
    watchlist = load_watchlist_data()
    
    if not watchlist:
        print("Warning: Watchlist is empty. Please add stocks using --add.")
    
    benchmark_symbol = config.get("benchmark", "SPY")
    
    # Fetch S&P 500 metadata
    sector_map = get_sp500_sectors()
    all_sp500_tickers = list(sector_map.keys())
    
    # Check for manual override from GitHub Action Inputs
    manual_input = os.getenv('MANUAL_TICKERS', '')
    if manual_input:
        full_scan_list = [t.strip().upper() for t in manual_input.split(',')]
        print(f"Manual override: Scanning {full_scan_list}")
    else:
        full_scan_list = list(set(watchlist + all_sp500_tickers))

    # 3. DATA ACQUISITION
    print(f"[1/4] Batch downloading data for {len(full_scan_list)} tickers...")
    try:
        batch_data = yf.download(full_scan_list, period="1y", interval="1d", group_by='ticker', threads=True)
        benchmark_data = yf.download(benchmark_symbol, period="1y")
    except Exception as e:
        print(f"Data download failed: {e}")
        return

    if benchmark_data.empty:
        print("Critical Error: Benchmark data missing.")
        return

    # Determine Market Regime via indicators.py
    regime_label = indicators.get_market_regime_label(benchmark_data)

    # 4. PROCESSING LOGIC
    print("[2/4] Analyzing and Grouping Stocks...")
    prev_state = state_manager.load_previous_state()
    current_full_state = {} # We will rebuild the state each run
    
    watchlist_data_for_plot = {}
    watchlist_reports = []
    sector_reports = {} 
    sector_leader_counts = {} 

    for ticker in full_scan_list:
        try:
            # Extract individual ticker data from batch
            if len(full_scan_list) > 1:
                # Handle MultiIndex column issue if it arises
                try:
                    stock_data = batch_data[ticker].dropna()
                except KeyError:
                    continue
            else:
                stock_data = batch_data.dropna()

            if stock_data.empty: continue

            # --- Technical Analysis (indicators.py) ---
            analyzed_data = indicators.calculate_metrics(stock_data, benchmark_data)
            
            # --- Scoring (scoring.py) ---
            rating_result = scoring.generate_rating(analyzed_data)
            
            # --- Alerting (state_manager.py) ---
            ticker_alerts = state_manager.get_ticker_alerts(ticker, analyzed_data, prev_state)
            current_full_state = state_manager.update_ticker_state(ticker, analyzed_data, current_full_state)

            # --- Formatting (telegram_notifier.py) ---
            report_line = telegram_notifier.format_ticker_report(ticker, ticker_alerts, analyzed_data.iloc[-1], rating_result)

            # --- Sorting Logic ---
            if ticker in watchlist:
                watchlist_reports.append(report_line)
                watchlist_data_for_plot[ticker] = analyzed_data
            else:
                # S&P 500 Leaders Filter (Only Tier 1 Leaders, Score >= 80)
                if rating_result['score'] >= 80:
                    sector = sector_map.get(ticker, "Other Sectors")
                    if sector not in sector_reports:
                        sector_reports[sector] = []
                    sector_reports[sector].append(report_line)
                    sector_leader_counts[sector] = sector_leader_counts.get(sector, 0) + 1

        except Exception as e:
            # print(f"Error processing {ticker}: {e}") # Optional: Uncomment for debugging
            continue

    # 5. REPORT GENERATION & NOTIFICATION
    print("[3/4] Compiling Executive Report...")
    
    # Watchlist Segment
    watchlist_segment = "📌 **PRIMARY WATCHLIST**\n"
    if watchlist_reports:
        watchlist_segment += "".join(watchlist_reports)
    else:
        watchlist_segment += "_No active data for watchlist._\n"

    # Sector Leaders Segment
    sector_segment = "📊 **S&P 500 MOMENTUM LEADERS**\n"
    if not sector_reports:
        sector_segment += "_No Tier 1 Leaders found today._"
    else:
        sorted_sectors = sorted(sector_leader_counts.items(), key=lambda x: x[1], reverse=True)
        for sector, count in sorted_sectors:
            sector_segment += f"\n📂 *{sector}* ({count})\n"
            sector_segment += "".join(sector_reports[sector][:3]) # Top 3 per sector

    # Dispatch to Telegram (telegram_notifier.py)
    final_payload = [watchlist_segment, sector_segment]
    telegram_notifier.send_bundle(final_payload, regime_label)

    # Save finalized state
    state_manager.save_current_state(current_full_state)
    
    # 6. VISUALIZATION (plotting.py)
    if watchlist_data_for_plot:
        print("[4/4] Generating Dashboards...")
        try:
            plotting.create_comparison_chart(watchlist_data_for_plot, benchmark_data)
        except Exception as e:
            print(f"Plotting error: {e}")

    print("\nJFO Engine: Analysis Complete.")

# ==========================================
# MAIN EXECUTION ENTRY POINT
# ==========================================

def main():
    parser = argparse.ArgumentParser(description="JFO Market Intelligence Engine")
    
    # Arguments for Watchlist Management
    parser.add_argument('--add', nargs='+', help="Add tickers to watchlist (e.g. --add AAPL MSFT)")
    parser.add_argument('--remove', nargs='+', help="Remove tickers from watchlist (e.g. --remove TSLA)")
    parser.add_argument('--list', action='store_true', help="Display current watchlist")
    
    # Argument for Analytics
    parser.add_argument('--analyze', action='store_true', help="Run the full analytics engine")

    args = parser.parse_args()

    # Priority 1: Management Commands
    if args.add or args.remove:
        manage_cli_updates(add_list=args.add, remove_list=args.remove)
    
    if args.list:
        wl = load_watchlist_data()
        print(f"Current Watchlist ({len(wl)}): {', '.join(wl)}")

    # Priority 2: Analytics
    # Run analytics if --analyze is passed, OR if NO arguments are passed (default behavior)
    if args.analyze or not any(vars(args).values()):
        run_analytics_engine()

if __name__ == "__main__":
    main()
