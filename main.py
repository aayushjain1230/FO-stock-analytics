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
STATE_DIR = 'state'
HASH_FILE = os.path.join(STATE_DIR, 'last_report_hash.json')

# ==========================================
# PART 1: WATCHLIST MANAGEMENT
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
# PART 2: UTILITIES & DEDUPLICATION
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
    
    if not os.path.exists(STATE_DIR):
        os.makedirs(STATE_DIR)

    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, 'r') as f:
            try:
                last_hash = json.load(f).get('hash')
                if last_hash == current_hash:
                    return False
            except:
                pass

    # Save new hash for the next comparison
    with open(HASH_FILE, 'w') as f:
        json.dump({'hash': current_hash}, f)
    return True

def get_sp500_sectors():
    try:
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
    if not is_market_open():
        print("Market is closed. Script terminating to save compute.")
        # return 

    # 2. INITIALIZATION
    config = load_config()
    if not config: return
    
    watchlist = load_watchlist_data()
    benchmark_symbol = config.get("benchmark", "SPY")
    sector_map = get_sp500_sectors()
    all_sp500_tickers = list(sector_map.keys())
    
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

    regime_label = indicators.get_market_regime_label(benchmark_data)

    # 4. PROCESSING LOGIC
    print("[2/4] Analyzing Leaders and Drops...")
    prev_state = state_manager.load_previous_state()
    current_full_state = {} 
    
    watchlist_data_for_plot = {}
    watchlist_reports = []
    leader_reports = {} 
    laggard_reports = {} # Stores Market Drops (Weakness)

    for ticker in full_scan_list:
        try:
            if len(full_scan_list) > 1:
                try:
                    stock_data = batch_data[ticker].dropna()
                except KeyError:
                    continue
            else:
                stock_data = batch_data.dropna()

            if stock_data.empty: continue

            # TA and Scoring
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
            if rating_result['score'] >= 80:
                if sector not in leader_reports: leader_reports[sector] = []
                leader_reports[sector].append(report_line)
            elif rating_result['score'] <= 30:
                if sector not in laggard_reports: laggard_reports[sector] = []
                laggard_reports[sector].append(report_line)

        except Exception:
            continue

    # 5. DEDUPLICATION & NOTIFICATION
    print("[3/4] Compiling Executive Report...")
    
    # Section A: Watchlist
    watchlist_segment = "📌 **PRIMARY WATCHLIST**\n" + ("".join(watchlist_reports) if watchlist_reports else "_No active data._\n")
    
    # Section B: Leaders
    leader_segment = "📈 **S&P 500 MOMENTUM LEADERS**\n"
    if not leader_reports:
        leader_segment += "_No Tier 1 Leaders found today._"
    else:
        for sec in sorted(leader_reports.keys()):
            leader_segment += f"\n📂 *{sec}*\n" + "".join(leader_reports[sec][:3])

    # Section C: Drops (New)
    drop_segment = "📉 **S&P 500 MARKET DROPS (WEAKNESS)**\n"
    if not laggard_reports:
        drop_segment += "_No significant drops found today._"
    else:
        for sec in sorted(laggard_reports.keys()):
            drop_segment += f"\n📂 *{sec}*\n" + "".join(laggard_reports[sec][:3])

    # Combine all text to fingerprint the run
    full_report_body = watchlist_segment + leader_segment + drop_segment
    
    if should_send_report(full_report_body):
        telegram_notifier.send_bundle([watchlist_segment, leader_segment, drop_segment], regime_label)
        print("New data detected. Notification sent to Telegram.")
    else:
        print("Data identical to last run. Staying silent to avoid spam.")

    # Save finalized state for alert tracking
    state_manager.save_current_state(current_full_state)
    
    # 6. VISUALIZATION
    if watchlist_data_for_plot:
        print("[4/4] Generating Dashboards...")
        try:
            plotting.create_comparison_chart(watchlist_data_for_plot, benchmark_data)
        except Exception as e:
            print(f"Plotting error: {e}")

    print("\nJFO Engine: Analysis Complete.")

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
