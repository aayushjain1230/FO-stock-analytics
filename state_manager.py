import json
import os
import pandas as pd
import numpy as np

# Path to the state file
STATE_FILE = os.path.join("state", "state.json")

def load_previous_state():
    """Loads the last recorded technical values from state.json."""
    if not os.path.exists(STATE_FILE):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        return {}

    try:
        with open(STATE_FILE, "r") as f:
            content = f.read().strip()
            if not content: # Handle empty file
                return {}
            return json.loads(content)
    except Exception as e:
        # If the JSON is corrupted, backup the bad file and start fresh
        print(f"Error loading state file: {e}. Starting with fresh state.")
        return {}

def save_current_state(full_state):
    """Saves current technical values for next run comparison with Type handling."""
    
    def json_type_fixer(obj):
        """Helper to convert NumPy types to Python native types for JSON."""
        if isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        if isinstance(obj, (np.int64, np.int32, int)):
            return int(obj)
        if isinstance(obj, (np.float64, np.float32, float)):
            return float(obj)
        return str(obj)

    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(full_state, f, indent=4, default=json_type_fixer)
    except Exception as e:
        print(f"Error saving state: {e}")

def get_ticker_alerts(ticker, current_data, previous_state):
    """
    Detects critical transitions only (not persistent conditions).
    """
    alerts = []

    if current_data is None or current_data.empty:
        return alerts

    latest = current_data.iloc[-1]
    prev = previous_state.get(ticker, {})

    if not prev:
        return ["✨ Initial data recorded. Monitoring for transitions."]

    # Use .item() or float() to ensure we aren't using numpy types in comparisons
    close = float(latest.get("Close", 0))
    sma50 = float(latest.get("SMA50", 0))
    sma200 = float(latest.get("SMA200", 0))
    rsi_w = float(latest.get("RSI_Weekly", 50))
    mrs = float(latest.get("MRS", 0))
    rv = float(latest.get("RV", 1.0))

    # -----------------------------
    # 1. STAGE 2 TRANSITION
    # -----------------------------
    is_stage_2 = bool(
        pd.notna(close)
        and pd.notna(sma50)
        and pd.notna(sma200)
        and close > sma50 > sma200
    )

    prev_stage_2 = bool(prev.get("is_stage_2", False))

    if is_stage_2 and not prev_stage_2:
        alerts.append("🚀 ENTERED STAGE 2: Perfect Trend Alignment (Bullish)")
    elif not is_stage_2 and prev_stage_2:
        alerts.append("⚠️ EXITED STAGE 2: Trend structure broken")

    # -----------------------------
    # 2. RELATIVE STRENGTH BREAKOUT
    # -----------------------------
    prev_mrs = float(prev.get("mrs", 0))

    if mrs > 0 and prev_mrs <= 0:
        alerts.append("⚡ RS BREAKOUT: Stock is now leading the market")
    elif mrs < 0 and prev_mrs >= 0:
        alerts.append("📉 RS BREAKDOWN: Stock is now lagging the market")

    # -----------------------------
    # 3. INSTITUTIONAL VOLUME
    # -----------------------------
    prev_rv = float(prev.get("rv", 1.0))
    if rv >= 2.0 and prev_rv < 2.0:
        alerts.append(f"📊 VOLUME SPIKE: {rv:.1f}x normal volume")

    # -----------------------------
    # 4. PRICE / SMA CROSSINGS
    # -----------------------------
    prev_close = float(prev.get("close", 0))
    prev_sma200 = float(prev.get("sma200", 0))
    prev_sma50 = float(prev.get("sma50", 0))

    if close > sma200 and prev_close <= prev_sma200:
        alerts.append(f"🚀 Crossed ABOVE SMA200 (${sma200:.2f})")
    elif close < sma200 and prev_close >= prev_sma200:
        alerts.append(f"🔴 Crossed BELOW SMA200 (${sma200:.2f})")

    if close > sma50 and prev_close <= prev_sma50:
        alerts.append(f"⚡ Crossed ABOVE SMA50 (${sma50:.2f})")

    # -----------------------------
    # 5. MOMENTUM SHIFT (RSI)
    # -----------------------------
    prev_rsi_w = float(prev.get("rsi_weekly", 0))
    if pd.notna(rsi_w) and rsi_w >= 50 and prev_rsi_w < 50:
        alerts.append("📈 Weekly RSI reclaimed 50 (Positive Momentum)")

    return alerts

def update_ticker_state(ticker, analyzed_data, current_full_state):
    """
    Stores only native Python types to ensure JSON compatibility.
    """
    if analyzed_data is None or analyzed_data.empty:
        return current_full_state

    latest = analyzed_data.iloc[-1]

    # Explicitly cast everything to float/bool to prevent JSON errors
    close = float(latest.get("Close", 0))
    sma50 = float(latest.get("SMA50", 0))
    sma200 = float(latest.get("SMA200", 0))
    rsi_w = float(latest.get("RSI_Weekly", 50))
    mrs = float(latest.get("MRS", 0))
    rv = float(latest.get("RV", 1.0))

    current_full_state[ticker] = {
        "close": close,
        "sma50": sma50,
        "sma200": sma200,
        "rsi_weekly": rsi_w,
        "mrs": mrs,
        "rv": rv,
        "is_stage_2": bool(close > sma50 > sma200 if pd.notna(close) and pd.notna(sma50) and pd.notna(sma200) else False),
    }

    return current_full_state
