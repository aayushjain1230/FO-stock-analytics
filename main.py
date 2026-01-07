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

def run_analytics_engine():
    print("--- Jain Family Office: US Stock Technical Engine v2 ---")
    
    # Check if we are running in the cloud (GitHub Actions)
    is_automated = os.getenv("GITHUB_ACTIONS") == "true"
    
    # 1. INITIALIZATION
    config = load_config()
    if not config:
        return

    # --- TICKER SELECTION ---
    if is_automated:
        # Automatically use config list in the cloud
        tickers = config.get("tickers", [])
        print(f"Running in Cloud: Using default config list {tickers}")
    else:
        # Interactive mode for local use
        while True:
            print("\n[PROMPT] Enter tickers to analyze separated by spaces (e.g., NVDA TSLA AAPL)")
            user_input = input("Leave blank to use config list (or type 'exit' to quit): ").strip()
            
            if user_input.lower() == 'exit':
                print("Exiting engine...")
                return

            if user_input:
                tickers = [t.strip().upper() for t in user_input.split() if t.strip()]
            else:
                tickers = config.get("tickers", [])
                print(f"Using default config list: {tickers}")

            if not tickers:
                print("[!] No tickers found. Please provide symbols to analyze.")
                continue

            # Validate tickers
            valid_tickers = []
            invalid_found = False
            
            print(f"Validating {len(tickers)} tickers...")
            for t in tickers:
                test_data = prices.get_price_history(t)
                if test_data is not None and not test_data.empty:
                    valid_tickers.append(t)
                else:
                    print(f"--- [ERROR] '{t}' is an invalid ticker. ---")
                    invalid_found = True
            
            if invalid_found:
                print("[!] Please re-enter the list with the correct spellings.")
                continue
            else:
                tickers = valid_tickers
                break

    benchmark_symbol = config.get("benchmark", "SPY")

    # 2. STATE MANAGEMENT & DATA COLLECTION SETUP
    prev_state = state_manager.load_previous_state()
    current_full_state = {}
    all_stock_data = {} 
    all_ticker_reports = [] # For bundled Telegram reporting

    # 3. BENCHMARK DATA
    print(f"\n[1/4] Fetching benchmark data ({benchmark_symbol})...")
    benchmark_data = prices.get_price_history(benchmark_symbol)
    
    if benchmark_data is None:
        print(f"Critical Error: Could not fetch {benchmark_symbol} data.")
        return

    # 4. PROCESSING LOOP
    print(f"[2/4] Processing {len(tickers)} tickers...")

    for ticker in tickers:
        print(f"\nAnalyzing: {ticker}...")
        
        stock_data = prices.get_price_history(ticker)
        
        # Core Analysis logic
        analyzed_data = indicators.calculate_metrics(stock_data, benchmark_data)
        rating_result = scoring.generate_rating(analyzed_data)
        
        all_stock_data[ticker] = analyzed_data
        
        # Detect Events (Changes in state)
        ticker_alerts = state_manager.get_ticker_alerts(ticker, analyzed_data, prev_state)
        
        # Update State Memory
        current_full_state = state_manager.update_ticker_state(ticker, analyzed_data, current_full_state)

        # Build individual Telegram block for this ticker
        ticker_report = telegram_notifier.format_ticker_report(ticker, ticker_alerts, analyzed_data.iloc[-1])
        all_ticker_reports.append(ticker_report)

        # Console Output
        print(f"  > Rating: {rating_result['rating']} ({rating_result['score']}/100)")
        if ticker_alerts:
            for alert in ticker_alerts:
                print(f"  > [EVENT] {alert}")

    # 5. STATE SAVING & NOTIFICATIONS
    print("\n[3/4] Saving state and sending bundled report...")
    state_manager.save_current_state(current_full_state)
    
    telegram_cfg = config.get("telegram", {})
    if telegram_cfg.get("enabled", True):
        telegram_notifier.send_bundle(all_ticker_reports) 
    else:
        print("  > Telegram notifications are disabled in config.")

    # 6. VISUALIZATION
    if all_stock_data:
        print("\n[4/4] Generating Executive Comparison Dashboard...")
        # Note: Plotting might fail in a headless GitHub environment without a virtual display
        try:
            plotting.create_comparison_chart(all_stock_data, benchmark_data)
        except Exception as e:
            print(f"  > Plotting skipped or failed: {e}")
    else:
        print("\n[4/4] No valid stock data to plot.")

    print("\nBatch analysis complete. Version 2 operational.")

if __name__ == "__main__":
    run_analytics_engine()
