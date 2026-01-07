import os
import json
import prices
import indicators
import scoring
import state_manager
import telegram_notifier 
import plotting 

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
    Saves the current index in state.json so each run 
    picks the next group of stocks from the market scan.
    """
    state = state_manager.load_previous_state()
    last_index = state.get("last_scan_index", 0)
    
    # Reset to 0 if we reached the end of the market scan list
    if last_index >= len(full_list):
        last_index = 0
        
    next_index = last_index + slice_size
    current_quart = full_list[last_index:next_index]
    
    # We don't save the index here; we pass it back to be saved with the full state later
    return current_quart, next_index

def run_analytics_engine():
    print("--- Jain Family Office: US Stock Technical Engine v2 ---")
    
    # Check if we are running in the cloud (GitHub Actions)
    is_automated = os.getenv("GITHUB_ACTIONS") == "true"
    
    # 1. INITIALIZATION
    config = load_config()
    if not config:
        return

    # --- TICKER SELECTION (WITH ROTATING QUART) ---
    if is_automated:
        watchlist = config.get("watchlist", [])
        market_scan_list = config.get("market_scan", [])
        
        # Get the next chunk of the S&P 500 / Market Scan
        quart_tickers, next_index = get_current_quart(market_scan_list, slice_size=25)
        
        # Combine Priority Watchlist + the Current Quart
        tickers = list(dict.fromkeys(watchlist + quart_tickers)) # Remove duplicates
        print(f"Running Cloud Scan: {len(watchlist)} priority + {len(quart_tickers)} from market scan.")
   else:
        # Define the recommended "Stage 2" list
        recommended_list = ["DOCN", "SE", "PATH", "CIEN", "NVDA", "LLY", "ORCL"]
        
        while True:
            print("\n" + "="*40)
            print("STOCK ANALYTICS ENGINE - INTERACTIVE MODE")
            print("="*40)
            print("Commands:")
            print("  - [Enter Tickers] e.g., 'AAPL TSLA NVDA'")
            print("  - 'rec'         : Use recommended Stage 2 list")
            print("  - 'reset'       : Clear watchlist and reset to defaults")
            print("  - 'exit'        : Close the program")
            print("-"*40)
            
            user_input = input("Selection: ").strip().lower()
            
            if user_input == 'exit':
                return
            
            # Feature 1: Reset Watchlist
            if user_input == 'reset':
                config['watchlist'] = []
                with open(os.path.join('config', 'config.json'), 'w') as f:
                    json.dump(config, f, indent=4)
                print("--- [SUCCESS] Watchlist reset to empty ---")
                continue

            # Feature 2: Recommended List
            if user_input == 'rec':
                tickers = recommended_list
                print(f"Using Recommended List: {tickers}")
                
                save_confirm = input("Save these to your permanent watchlist? (y/n): ").lower()
                if save_confirm == 'y':
                    config['watchlist'] = tickers
                    with open(os.path.join('config', 'config.json'), 'w') as f:
                        json.dump(config, f, indent=4)
                    print("--- [SUCCESS] Recommended list saved to config.json ---")
            
            # Manual Ticker Entry
            elif user_input:
                tickers = [t.strip().upper() for t in user_input.split() if t.strip()]
                save_confirm = input(f"Save {tickers} as your permanent watchlist? (y/n): ").lower()
                if save_confirm == 'y':
                    config['watchlist'] = tickers
                    with open(os.path.join('config', 'config.json'), 'w') as f:
                        json.dump(config, f, indent=4)
                    print("--- [SUCCESS] New watchlist saved ---")
            else:
                tickers = config.get("watchlist", [])
                print(f"Using current watchlist from config: {tickers}")

            if not tickers:
                print("[!] No tickers provided. Please enter symbols or type 'rec'.")
                continue
            break

    benchmark_symbol = config.get("benchmark", "SPY")

    # 2. STATE MANAGEMENT & DATA COLLECTION SETUP
    prev_state = state_manager.load_previous_state()
    current_full_state = {}
    
    # If in cloud, preserve the rotation index for the next run
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
        print(f"Analyzing: {ticker}...")
        stock_data = prices.get_price_history(ticker)
        
        if stock_data is None or stock_data.empty:
            continue

        analyzed_data = indicators.calculate_metrics(stock_data, benchmark_data)
        rating_result = scoring.generate_rating(analyzed_data)
        all_stock_data[ticker] = analyzed_data
        
        ticker_alerts = state_manager.get_ticker_alerts(ticker, analyzed_data, prev_state)
        current_full_state = state_manager.update_ticker_state(ticker, analyzed_data, current_full_state)

        ticker_report = telegram_notifier.format_ticker_report(ticker, ticker_alerts, analyzed_data.iloc[-1])
        all_ticker_reports.append(ticker_report)

    # 5. STATE SAVING & NOTIFICATIONS
    print("\n[3/4] Saving state and sending reports...")
    state_manager.save_current_state(current_full_state)
    
    telegram_cfg = config.get("telegram", {})
    if telegram_cfg.get("enabled", True):
        telegram_notifier.send_bundle(all_ticker_reports) 

    # 6. VISUALIZATION
    if all_stock_data:
        print("\n[4/4] Generating Dashboards...")
        try:
            plotting.create_comparison_chart(all_stock_data, benchmark_data)
        except Exception as e:
            print(f"Plotting error: {e}")

    print("\nBatch analysis complete. Rotating scanner operational.")

if __name__ == "__main__":
    run_analytics_engine()


