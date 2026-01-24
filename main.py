import os
import json
import prices
import indicators
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
            next_index = state_manager.load_previous_state().get("last_scan_index", 0)
        else:
            watchlist = config.get("watchlist", [])
            market_scan_list = config.get("market_scan", [])
            quart_tickers, next_index = get_current_quart(market_scan_list, slice_size=25)
            tickers = list(dict.fromkeys(watchlist + quart_tickers))
            print(f"Running Scheduled Scan: {len(watchlist)} priority + {len(quart_tickers)} scan.")
        
    else:
        # Local Interactive Mode
        recommended_list = ["DOCN", "SE", "PATH", "CIEN", "NVDA", "LLY", "ORCL"]
        while True:
            print("\n" + "="*40)
            print("STOCK ANALYTICS ENGINE - INTERACTIVE MODE")
            print("="*40)
            user_input = input("Selection (tickers, 'rec', 'reset', 'exit'): ").strip().lower()
            
            if user_input == 'exit': return
            if user_input == 'reset':
                config['watchlist'] = []
                with open(os.path.join('config', 'config.json'), 'w') as f: json.dump(config, f, indent=4)
                print("--- Watchlist reset ---"); continue

            if user_input == 'rec':
                tickers = recommended_list
            elif user_input:
                tickers = [t.strip().upper() for t in user_input.split() if t.strip()]
            else:
                tickers = config.get("watchlist", [])

            if not tickers:
                print("[!] No tickers provided.")
                continue
            break

    benchmark_symbol = config.get("benchmark", "SPY")

    # 2. STATE & DATA SETUP
    prev_state = state_manager.load_previous_state()
    current_full_state = {}
    
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
            # 1. Run Indicators
            analyzed_data = indicators.calculate_metrics(stock_data, benchmark_data)
            
            # 2. Apply "Market Leader" Scoring (V2 New Logic)
            last_row = analyzed_data.iloc[-1]
            score = indicators.calculate_market_leader_score(last_row)
            analyzed_data['Market_Leader_Score'] = score # Apply to the latest record
            
            all_stock_data[ticker] = analyzed_data
            
            # 3. Handle Alerts & Memory
            ticker_alerts = state_manager.get_ticker_alerts(ticker, analyzed_data, prev_state)
            current_full_state = state_manager.update_ticker_state(ticker, analyzed_data, current_full_state)

            # 4. Format for Telegram (Passing the score into the report)
            ticker_report = telegram_notifier.format_ticker_report(
                ticker, 
                ticker_alerts, 
                last_row, 
                score=score
            )
            all_ticker_reports.append(ticker_report)
            
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    # 5. STATE SAVING & NOTIFICATIONS
    print("\n[3/4] Saving state and sending reports...")
    state_manager.save_current_state(current_full_state)
    
    telegram_cfg = config.get("telegram", {})
    if telegram_cfg.get("enabled", True) and all_ticker_reports:
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
