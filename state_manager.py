import json
import os
import pandas as pd

# Path to the state file - Ensures persistence in the 'state' directory
STATE_FILE = os.path.join('state', 'state.json')

def load_previous_state():
    """Loads the last recorded technical values from state.json."""
    if not os.path.exists(STATE_FILE):
        # Create directory if it doesn't exist to prevent FileNotFoundError
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
        # Ensure directory exists before saving
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, 'w') as f:
            json.dump(full_state, f, indent=4)
    except Exception as e:
        print(f"Error saving state: {e}")

def get_ticker_alerts(ticker, current_data, previous_state):
    """
    Detects critical transitions: Stage changes, RS breakouts, and Trend shifts.
    Compares current metrics against the saved state to trigger 'new' alerts only.
    """
    alerts = []
    
    # Safety Check: Get the very last row of analyzed data
    if current_data is None or current_data.empty:
        return alerts
        
    latest = current_data.iloc[-1]
    
    # Get previous values from state (if they exist)
    prev = previous_state.get(ticker, {})
    
    if not prev:
        # Avoid flooding on the first-ever run for a ticker
        return ["✨ Initial data recorded. Monitoring for transitions."]

    # --- 1. Stage 2 Transition (The Stan Weinstein Methodology) ---
    is_stage_2 = bool(latest['Close'] > latest['SMA50'] > latest['SMA200'])
    was_stage_2 = prev.get('is_stage_2', False)
    
    if is_stage_2 and not was_stage_2:
        alerts.append("🚀 ENTERED STAGE 2: Perfect Trend Alignment (Bullish)")
    elif not is_stage_2 and was_stage_2:
        alerts.append("⚠️ EXITED STAGE 2: Trend structure broken")

    # --- 2. Mansfield Relative Strength (MRS) Breakout ---
    # Captures the exact moment a stock starts outperforming the S&P 500
    mrs = latest.get('MRS', 0)
    prev_mrs = prev.get('mrs', 0)
    if mrs > 0 and prev_mrs <= 0:
        alerts.append("⚡ RS BREAKOUT: Stock is now leading the market")
    elif mrs < 0 and prev_mrs >= 0:
        alerts.append("📉 RS BREAKDOWN: Stock is now lagging the market")

    # --- 3. Institutional Volume Spikes ---
    rv = latest.get('RV', 1.0)
    if rv >= 2.0: # 2x the 20-day average volume
        alerts.append(f"📊 VOLUME SPIKE: {rv:.1f}x normal volume (Institutional Buy)")

    # --- 4. Price vs SMA Crossings ---
    # Long-term Trend (SMA200)
    if latest['Close'] > latest['SMA200'] and prev.get('close', 0) <= prev.get('sma200', 0):
        alerts.append(f"🚀 Crossed ABOVE SMA200 (${latest['SMA200']:.2f})")
    elif latest['Close'] < latest['SMA200'] and prev.get('close', 0) >= prev.get('sma200', 0):
        alerts.append(f"🔴 Crossed BELOW SMA200 (${latest['SMA200']:.2f})")

    # Medium-term Trend (SMA50)
    if latest['Close'] > latest['SMA50'] and prev.get('close', 0) <= prev.get('sma50', 0):
        alerts.append(f"⚡ Crossed ABOVE SMA50 (${latest['SMA50']:.2f})")

    # --- 5. Momentum Shifts (RSI) ---
    # Reclaiming the 50-midline is a sign of renewed strength
    if latest['RSI_Weekly'] >= 50 and prev.get('rsi_weekly', 0) < 50:
        alerts.append("📈 Weekly RSI reclaimed 50 (Positive Momentum)")

    # --- 6. 52-Week High Breakouts ---
    # Using a 252-day window for high detection
    rolling_high = current_data['Close'].rolling(window=252, min_periods=1).max().iloc[-1]
    if latest['Close'] >= rolling_high and latest['Close'] > prev.get('close', 0):
         alerts.append(f"🔥 BLUE SKY: New 52-Week High at ${latest['Close']:.2f}")

    return alerts

def update_ticker_state(ticker, analyzed_data, current_full_state):
    """
    Populates the state dictionary with current values for JSON persistence.
    Converts all NumPy/Pandas types to standard Python types for JSON compatibility.
    """
    if analyzed_data is None or analyzed_data.empty:
        return current_full_state
        
    latest = analyzed_data.iloc[-1]
    
    # Store only what is required for comparison logic in the next run
    current_full_state[ticker] = {
        "close": float(latest['Close']),
        "sma200": float(latest['SMA200']),
        "sma50": float(latest['SMA50']),
        "rsi_weekly": float(latest['RSI_Weekly']) if pd.notnull(latest['RSI_Weekly']) else 50.0,
        "mrs": float(latest.get('MRS', 0)),
        "is_stage_2": bool(latest['Close'] > latest['SMA50'] > latest['SMA200']),
        "rv": float(latest.get('RV', 1.0))
    }
    return current_full_state
