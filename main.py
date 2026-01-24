import os
import json
import prices
import indicators
import state_manager
import telegram_notifier 
import plotting 
import scoring  # Integrated the new scoring module

def load_config():
    """Loads settings and ticker list from config/config.json."""
    config_path = os.path.join('config', 'config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: config.json is not formatted correctly.")
        return None

def get_current_quart(full_list, slice_size=25):
    """
    Saves the current index in state so each run 
    picks the next group of stocks from the market scan.
    """
    state = state_manager.load_previous_state()
    last_index = state.get("last_scan_index", 0)
    
    if last_index >= len(full_list):
        last_index = 0
        
    next_index = last_index + slice_size
    current_quart = full_list[last_index:next_index]
    
    return current_quart, next_index

def run_analytics_engine():
    print("--- Jain Family Office: US Stock Technical Engine v2 ---")
    
    is_automated = os.getenv("GITHUB_ACTIONS") == "true"
    
    # 1. INITIALIZATION
    config = load_config()
    if not config:
        return

    # --- TICKER SELECTION ---
    if is_automated:
        manual_input = os.getenv("MANUAL_TICKERS", "").strip()
        if manual_input:
            tickers = [t.strip().upper() for t in manual_input.split() if t.strip()]
            print(f"Running Manual Cloud Prompt: {tickers}")
            # Keep the current index where it is if running a manual prompt
            next_index = state_manager.load_previous_state().get("last_scan_index", 0)
        else:
            watchlist = config.get("watchlist", [])
            market_scan_list = config.get("market_scan", [])
            quart_tickers, next_index = get_current_quart(market_scan_list, slice_size=25)
            # Combine watchlist (always scan) + the current rotating slice
            tickers = list(dict.fromkeys(watchlist + quart_tickers))
            print(f"Running Scheduled Scan: {len(watchlist)} priority + {len(quart_tickers)} scan.")
    else:
        # Local Interactive Mode (kept for your local testing)
        tickers = config.get("watchlist", ["AAPL", "NVDA", "TSLA"])
        next_index = 0

    benchmark_symbol = config.get("benchmark", "SPY")

    # 2. STATE SETUP
    # We load the full previous state so we can preserve data for tickers 
    # that ARE NOT in the current scan group.
    prev_state = state_manager.load_previous_state()
    current_full_state = prev_state.copy() 
    
    if is_automated:
        current_full_state["last_scan_index"] = next_index

    all_stock_data = {} 
    all_ticker_reports = [] 

    # 3. BENCHMARK DATA
    print(f"\n[1/4] Fetching benchmark data ({benchmark_symbol})...")
    benchmark_data = prices.get_price_history(benchmark_symbol)
    if benchmark_data is None:
        print(f"Critical Error: Could not fetch benchmark data.")
        return

    # 4. PROCESSING LOOP
    print(f"[2/4] Processing {len(tickers)} tickers...")

    for ticker in tickers:
        try:
            print(f"Analyzing: {ticker}...")
            stock_data = prices.get_price_history(ticker)
            
            if stock_data is None or stock_data.empty:
                continue

            # --- Calculation Core ---
            # 1. Run Indicators (Math logic)
            analyzed_data = indicators.calculate_metrics(stock_data, benchmark_data)
            
            # 2. Apply "Market Leader" Scoring (V2 New Logic from scoring.py)
            rating_result = scoring.generate_rating(analyzed_data)
            score = rating_result['score']
            
            all_stock_data[ticker] = analyzed_data
            
            # 3. Handle Alerts & Update memory for this specific ticker
            ticker_alerts = state_manager.get_ticker_alerts(ticker, analyzed_data, prev_state)
            current_full_state = state_manager.update_ticker_state(ticker, analyzed_data, current_full_state)

            # 4. Format for Telegram
            ticker_report = telegram_notifier.format_ticker_report(
                ticker, 
                ticker_alerts, 
                analyzed_data.iloc[-1], 
                score=score
            )
            all_ticker_reports.append(ticker_report)
            
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    # 5. STATE SAVING & NOTIFICATIONS
    print("\n[3/4] Saving state and sending reports...")
    state_manager.save_current_state(current_full_state)
    
    if all_ticker_reports:
        telegram_notifier.send_bundle(all_ticker_reports) 

    # 6. VISUALIZATION
    if all_stock_data:
        print("\n[4/4] Generating Dashboards...")
        try:
            plotting.create_comparison_chart(all_stock_data, benchmark_data)
        except Exception as e:
            print(f"Plotting error: {e}")

    print("\nBatch analysis complete. Scanner updated.")

if __name__ == "__main__":
    run_analytics_engine()
