import json
import os

# Path to the state file
STATE_FILE = os.path.join('state', 'state.json')

def load_previous_state():
    """Loads the last recorded technical values from state.json."""
    if not os.path.exists(STATE_FILE):
        # Ensure directory exists for Version 2 persistence
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        return {}
    
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading state file: {e}")
        return {}

def save_current_state(full_state):
    """Saves current technical values for next run comparison."""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(full_state, f, indent=4)
    except Exception as e:
        print(f"Error saving state: {e}")

def get_ticker_alerts(ticker, current_data, previous_state):
    """
    Compares previous vs current values to detect technical events.
    Returns a list of alert messages for the ticker based on state transitions.
    """
    alerts = []
    
    # Get current values (the very last row of analyzed data)
    latest = current_data.iloc[-1]
    
    # Get previous values from state (if they exist)
    prev = previous_state.get(ticker, {})
    
    if not prev:
        # Handle initial state for new tickers
        return ["Initial data recorded. Monitoring for changes starting next run."]

    # 1. Price vs SMA Crossings (SMA200 and SMA50)
    # Check SMA200 (Long-term Trend)
    if latest['Close'] > latest['SMA200'] and prev.get('close', 0) <= prev.get('sma200', 0):
        alerts.append(f"🚀 Price crossed ABOVE SMA200 (${latest['SMA200']:.2f})")
    elif latest['Close'] < latest['SMA200'] and prev.get('close', 0) >= prev.get('sma200', 0):
        alerts.append(f"⚠️ Price crossed BELOW SMA200 (${latest['SMA200']:.2f})")

    # Check SMA50 (Medium-term Trend)
    if latest['Close'] > latest['SMA50'] and prev.get('close', 0) <= prev.get('sma50', 0):
        alerts.append(f"⚡ Price crossed ABOVE SMA50 (${latest['SMA50']:.2f})")
    elif latest['Close'] < latest['SMA50'] and prev.get('close', 0) >= prev.get('sma50', 0):
        alerts.append(f"📉 Price crossed BELOW SMA50 (${latest['SMA50']:.2f})")

    # 2. Weekly RSI Momentum (Bullish/Bearish Thresholds)
    # Bullish Cross: Above 40
    if latest['RSI_Weekly'] >= 40 and prev.get('rsi_weekly', 0) < 40:
        alerts.append(f"📈 Weekly RSI reclaimed 40 (Bullish Momentum)")
    # Bearish Cross: Below 40
    elif latest['RSI_Weekly'] < 40 and prev.get('rsi_weekly', 0) >= 40:
        alerts.append(f"📉 Weekly RSI dropped below 40 (Bearish Shift)")

    # 3. Monthly RSI Momentum
    # Alert if Monthly RSI crosses above 40
    if latest['RSI_Monthly'] >= 40 and prev.get('rsi_monthly', 0) < 40:
        alerts.append(f"🌟 Monthly RSI improved above 40 (Now: {latest['RSI_Monthly']:.1f})")

    # 4. New 52-Week Highs and Lows
    # 52-Week High Breakout
    if latest['Close'] >= latest['52W_High'] and latest['Close'] > prev.get('52w_high', 0):
        alerts.append(f"🔥 NEW 52-Week High reached at ${latest['Close']:.2f}")
    
    # 52-Week Low Breakout (If tracked in indicators)
    if '52W_Low' in latest:
        if latest['Close'] <= latest['52W_Low'] and latest['Close'] < prev.get('52w_low', 999999):
            alerts.append(f"🧊 NEW 52-Week Low reached at ${latest['Close']:.2f}")

    return alerts

def update_ticker_state(ticker, analyzed_data, current_full_state):
    """
    Helper to update the memory with current values for the next run.
    Stores all required metrics to enable crossover detection in the next cycle.
    """
    latest = analyzed_data.iloc[-1]
    current_full_state[ticker] = {
        "close": latest['Close'],
        "sma200": latest['SMA200'],
        "sma50": latest['SMA50'],
        "rsi_weekly": latest['RSI_Weekly'],
        "rsi_monthly": latest['RSI_Monthly'],
        "52w_high": latest['52W_High'],
        "52w_low": latest.get('52W_Low', 0)
    }
    return current_full_state