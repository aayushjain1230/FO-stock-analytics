import json
import os
import sys

WATCHLIST_FILE = 'watchlist.json'

def load_watchlist():
    if os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE, 'r') as f:
            return json.load(f)
    return []

def save_watchlist(watchlist):
    with open(WATCHLIST_FILE, 'w') as f:
        json.dump(list(set(watchlist)), f, indent=4) # set() removes duplicates

def main():
    action = os.getenv('INPUT_ACTION')
    stocks_input = os.getenv('INPUT_STOCKS', '')
    
    # Convert comma-separated string to a clean list
    new_stocks = [s.strip().upper() for s in stocks_input.split(',') if s.strip()]
    
    current_watchlist = load_watchlist()

    if action == "Add to Watchlist":
        current_watchlist.extend(new_stocks)
        print(f"Added {new_stocks} to watchlist.")
    elif action == "Overwrite Watchlist":
        current_watchlist = new_stocks
        print(f"Watchlist reset to: {new_stocks}")
    else:
        print(f"Running analysis for: {current_watchlist}")

    save_watchlist(current_watchlist)

if __name__ == "__main__":
    main()
