import json
import os
import pandas as pd

# Path to the state file
STATE_FILE = os.path.join("state", "state.json")


def load_previous_state():
    """Loads the last recorded technical values from state.json."""
    if not os.path.exists(STATE_FILE):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        return {}

    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading state file: {e}")
        return {}


def save_current_state(full_state):
    """Saves current technical values for next run comparison."""
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(full_state, f, indent=4)
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

    close = latest.get("Close")
    sma50 = latest.get("SMA50")
    sma200 = latest.get("SMA200")
    rsi_w = latest.get("RSI_Weekly")
    mrs = latest.get("MRS", 0)
    rv = latest.get("RV", 1.0)

    # -----------------------------
    # 1. STAGE 2 TRANSITION
    # -----------------------------
    is_stage_2 = (
        pd.notna(close)
        and pd.notna(sma50)
        and pd.notna(sma200)
        and close > sma50 > sma200
    )

    if is_stage_2 and not prev.get("is_stage_2", False):
        alerts.append("🚀 ENTERED STAGE 2: Perfect Trend Alignment (Bullish)")
    elif not is_stage_2 and prev.get("is_stage_2", False):
        alerts.append("⚠️ EXITED STAGE 2: Trend structure broken")

    # -----------------------------
    # 2. RELATIVE STRENGTH BREAKOUT
    # -----------------------------
    prev_mrs = prev.get("mrs", 0)

    if mrs > 0 and prev_mrs <= 0:
        alerts.append("⚡ RS BREAKOUT: Stock is now leading the market")
    elif mrs < 0 and prev_mrs >= 0:
        alerts.append("📉 RS BREAKDOWN: Stock is now lagging the market")

    # -----------------------------
    # 3. INSTITUTIONAL VOLUME (TRANSITION ONLY)
    # -----------------------------
    prev_rv = prev.get("rv", 1.0)

    if rv >= 2.0 and prev_rv < 2.0:
        alerts.append(f"📊 VOLUME SPIKE: {rv:.1f}x normal volume")

    # -----------------------------
    # 4. PRICE / SMA CROSSINGS
    # -----------------------------
    prev_close = prev.get("close", 0)
    prev_sma200 = prev.get("sma200", 0)
    prev_sma50 = prev.get("sma50", 0)

    if close > sma200 and prev_close <= prev_sma200:
        alerts.append(f"🚀 Crossed ABOVE SMA200 (${sma200:.2f})")
    elif close < sma200 and prev_close >= prev_sma200:
        alerts.append(f"🔴 Crossed BELOW SMA200 (${sma200:.2f})")

    if close > sma50 and prev_close <= prev_sma50:
        alerts.append(f"⚡ Crossed ABOVE SMA50 (${sma50:.2f})")

    # -----------------------------
    # 5. MOMENTUM SHIFT (RSI)
    # -----------------------------
    if pd.notna(rsi_w) and rsi_w >= 50 and prev.get("rsi_weekly", 0) < 50:
        alerts.append("📈 Weekly RSI reclaimed 50 (Positive Momentum)")

    # -----------------------------
    # 6. 52-WEEK HIGH BREAKOUT
    # -----------------------------
    rolling_high = (
        current_data["Close"]
        .rolling(window=252, min_periods=1)
        .max()
        .iloc[-1]
    )

    if close > rolling_high * 0.999 and close > prev_close:
        alerts.append(f"🔥 BLUE SKY: New 52-Week High at ${close:.2f}")

    return alerts


def update_ticker_state(ticker, analyzed_data, current_full_state):
    """
    Stores only what is needed for next-run comparisons.
    """
    if analyzed_data is None or analyzed_data.empty:
        return current_full_state

    latest = analyzed_data.iloc[-1]

    close = latest.get("Close")
    sma50 = latest.get("SMA50")
    sma200 = latest.get("SMA200")
    rsi_w = latest.get("RSI_Weekly")
    mrs = latest.get("MRS", 0)
    rv = latest.get("RV", 1.0)

    current_full_state[ticker] = {
        "close": float(close) if pd.notna(close) else 0.0,
        "sma50": float(sma50) if pd.notna(sma50) else 0.0,
        "sma200": float(sma200) if pd.notna(sma200) else 0.0,
        "rsi_weekly": float(rsi_w) if pd.notna(rsi_w) else 50.0,
        "mrs": float(mrs),
        "rv": float(rv),
        "is_stage_2": (
            pd.notna(close)
            and pd.notna(sma50)
            and pd.notna(sma200)
            and close > sma50 > sma200
        ),
    }

    return current_full_state
