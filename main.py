import os
import json
import sys
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import yfinance as yf
from datetime import datetime

# Existing project modules
import prices
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
    # Use UTC for GitHub Actions compatibility
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
        # Wikipedia is the most reliable free source for GICS sectors
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        # Standardize tickers (e.g., BRK.B -> BRK-B) for yfinance
        table['Symbol'] = table['Symbol'].str.replace('.', '-', regex=False)
        return dict(zip(table['Symbol'], table['GICS Sector']))
    except Exception as e:
        print(f"Error fetching S&P 500 sector list: {e}")
        return {}

def run_analytics_engine():
    print("\n--- Jain Family Office: Market Intelligence Engine v2 ---")
    
    # 1. MARKET GATEKEEPER
    # Prevents running on weekends, holidays, or after hours.
    if not is_market_open():
        print("Market is closed. Script terminating to save GitHub Actions minutes.")
        return

    # 2. INITIALIZATION
    config = load_config()
    if not config:
        return
    
    watchlist = config.get("watchlist", [])
    benchmark_symbol = config.get("benchmark", "SPY")
    
    # Fetch S&P 500 metadata
    sector_map = get_sp500_sectors()
    all_sp500_tickers = list(sector_map.keys())
    
    # Create a unique list of all tickers to download
    full_scan_list = list(set(watchlist + all_sp500_tickers))

    # 3. DATA ACQUISITION
    print(f"[1/4] Batch downloading data for {len(full_scan_list)} tickers...")
    # 'threads=True' is critical for speed when handling 500+ tickers
    batch_data = yf.download(full_scan_list, period="1y", interval="1d", group_by='ticker', threads=True)
    benchmark_data = prices.get_price_history(benchmark_symbol)

    if benchmark_data is None:
        print("Critical Error: Could not fetch benchmark data.")
        return

    # 4. PROCESSING LOGIC
    print("[2/4] Analyzing and Grouping Stocks...")
    prev_state = state_manager.load_previous_state()
    current_full_state = prev_state.copy()
    
    watchlist_data_for_plot = {}
    watchlist_reports = []
    sector_reports = {} # Structure: { "Information Technology": [lines], "Energy": [lines] }

    for ticker in full_scan_list:
        try:
            # Extract specific ticker data from the multi-indexed batch dataframe
            stock_data = batch_data[ticker].dropna()
            if stock_data.empty:
                continue

            # --- Analysis Pipeline ---
            analyzed_data = indicators.calculate_metrics(stock_data, benchmark_data)
            rating = scoring.generate_rating(analyzed_data)
            score = rating['score']
            latest_metrics = analyzed_data.iloc[-1]
            
            # --- Alert & State Management ---
            ticker_alerts = state_manager.get_ticker_alerts(ticker, analyzed_data, prev_state)
            current_full_state = state_manager.update_ticker_state(ticker, analyzed_data, current_full_state)

            # --- Formatting ---
            report_line = telegram_notifier.format_ticker_report(ticker, ticker_alerts, latest_metrics, score)

            # --- Sorting Logic ---
            if ticker in watchlist:
                # Priority 1: Always include in Watchlist section
                watchlist_reports.append(report_line)
                watchlist_data_for_plot[ticker] = analyzed_data
            else:
                # Priority 2: Include in Sector report ONLY if score is high (Signal > Noise)
                if score >= 70:
                    sector = sector_map.get(ticker, "Other Sectors")
                    if sector not in sector_reports:
                        sector_reports[sector] = []
                    sector_reports[sector].append(report_line)

        except Exception as e:
            # Silently skip errors to ensure the rest of the 500 tickers complete
            continue

    # 5. REPORT GENERATION & NOTIFICATION
    print("[3/4] Compiling Clean Report...")
    final_report = "🚀 **DAILY MARKET INTELLIGENCE** 🚀\n\n"
    
    # Section A: Watchlist
    final_report += "📌 **PRIMARY WATCHLIST**\n"
    if watchlist_reports:
        final_report += "\n".join(watchlist_reports)
    else:
        final_report += "_No data found for watchlist symbols._"
    
    final_report += "\n\n" + ("━" * 15) + "\n\n"
    
    # Section B: Grouped S&P 500 Leaders
    final_report += "📊 **S&P 500 SECTOR LEADERS** (Score > 70)\n"
    if not sector_reports:
        final_report += "_No high-scoring stocks found today._"
    else:
        # Sort sectors alphabetically for a clean UI
        for sector in sorted(sector_reports.keys()):
            reports = sector_reports[sector]
            final_report += f"\n📂 *{sector}*\n"
            # Limit to top 5 per sector to avoid hitting Telegram message size limits
            final_report += "\n".join(reports[:5])
            final_report += "\n"

    # Save indices and states
    state_manager.save_current_state(current_full_state)
    
    # Send via the 'long message' helper to handle character limits
    telegram_notifier.send_long_message(final_report)

    # 6. VISUALIZATION
    # We only plot the watchlist to keep the dashboard high-resolution and focused
    if watchlist_data_for_plot:
        print("[4/4] Generating Watchlist Dashboards...")
        try:
            plotting.create_comparison_chart(watchlist_data_for_plot, benchmark_data)
        except Exception as e:
            print(f"Plotting error: {e}")

    print("\nBatch analysis complete. Telegram report dispatched.")

if __name__ == "__main__":
    run_analytics_engine()
