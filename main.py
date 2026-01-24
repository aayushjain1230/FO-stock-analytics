import os
import json
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

def load_config():
    """Loads settings and ticker list from config/config.json."""
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
    """Scrapes Wikipedia for the live S&P 500 list and GICS sectors."""
    try:
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        table['Symbol'] = table['Symbol'].str.replace('.', '-', regex=False)
        return dict(zip(table['Symbol'], table['GICS Sector']))
    except Exception as e:
        print(f"Error fetching S&P 500 sector list: {e}")
        return {}

def run_analytics_engine():
    print("\n--- Jain Family Office: Market Intelligence Engine v2 ---")
    
    # 1. MARKET GATEKEEPER
    # Checks if NYSE is open. (Comment out the 'return' for weekend testing)
    if not is_market_open():
        print("Market is closed. Script terminating to save compute.")
        # return 

    # 2. INITIALIZATION
    config = load_config()
    if not config: return
    
    watchlist = config.get("watchlist", [])
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
    batch_data = yf.download(full_scan_list, period="1y", interval="1d", group_by='ticker', threads=True)
    benchmark_data = yf.download(benchmark_symbol, period="1y")

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
                stock_data = batch_data[ticker].dropna()
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
            print(f"Error processing {ticker}: {e}")
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

if __name__ == "__main__":
    run_analytics_engine()
