import json
import os

import numpy as np
import pandas as pd


STATE_FILE = os.path.join("state", "state.json")

DEFAULT_SETTINGS = {
    "sma_fast": 50,
    "sma_slow": 200,
    "rsi_weekly_breakdown_threshold": 40,
    "rsi_monthly_breakout_threshold": 40,
    "relative_volume_alert_threshold": 2.0,
}


def _get_settings(config):
    settings = dict(DEFAULT_SETTINGS)
    if isinstance(config, dict):
        settings.update(config.get("settings", {}))
    return settings


def _safe_float(value, default=None):
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _crossed_above(current_value, previous_value, current_reference, previous_reference):
    values = (current_value, previous_value, current_reference, previous_reference)
    if any(value is None for value in values):
        return False
    return current_value > current_reference and previous_value <= previous_reference


def _crossed_below(current_value, previous_value, current_reference, previous_reference):
    values = (current_value, previous_value, current_reference, previous_reference)
    if any(value is None for value in values):
        return False
    return current_value < current_reference and previous_value >= previous_reference


def load_previous_state():
    """Load the previous state payload from disk."""
    if not os.path.exists(STATE_FILE):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        return {}

    try:
        with open(STATE_FILE, "r") as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except Exception as e:
        print(f"Error loading state file: {e}. Starting with fresh state.")
        return {}


def save_current_state(full_state):
    """Save the current state using JSON-safe native values."""

    def json_type_fixer(obj):
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


def get_ticker_alerts(ticker, current_data, previous_state, config=None):
    """Detect state transitions between the previous and current runs."""
    alerts = []

    if current_data is None or current_data.empty:
        return alerts

    latest = current_data.iloc[-1]
    previous = previous_state.get(ticker, {})
    settings = _get_settings(config)

    if not previous:
        return ["Initial data recorded. Monitoring for transitions."]

    close = _safe_float(latest.get("Close"))
    sma_fast_value = _safe_float(latest.get("SMA50"))
    sma_slow_value = _safe_float(latest.get("SMA200"))
    weekly_rsi = _safe_float(latest.get("RSI_Weekly"))
    monthly_rsi = _safe_float(latest.get("RSI_Monthly"))
    relative_strength = _safe_float(latest.get("MRS"), 0.0)
    relative_volume = _safe_float(latest.get("RV"), 1.0)
    high_52w = _safe_float(latest.get("High_52W"))
    low_52w = _safe_float(latest.get("Low_52W"))

    prev_close = _safe_float(previous.get("close"))
    prev_sma_fast = _safe_float(previous.get("sma50"))
    prev_sma_slow = _safe_float(previous.get("sma200"))
    prev_weekly_rsi = _safe_float(previous.get("rsi_weekly"))
    prev_monthly_rsi = _safe_float(previous.get("rsi_monthly"))
    prev_relative_strength = _safe_float(previous.get("mrs"), 0.0)
    prev_relative_volume = _safe_float(previous.get("rv"), 1.0)
    prev_high_52w = _safe_float(previous.get("high_52w"))
    prev_low_52w = _safe_float(previous.get("low_52w"))

    sma_fast = int(settings["sma_fast"])
    sma_slow = int(settings["sma_slow"])
    weekly_breakdown = float(settings["rsi_weekly_breakdown_threshold"])
    monthly_breakout = float(settings["rsi_monthly_breakout_threshold"])
    relative_volume_threshold = float(settings["relative_volume_alert_threshold"])

    is_stage_2 = bool(
        pd.notna(close)
        and pd.notna(sma_fast_value)
        and pd.notna(sma_slow_value)
        and close > sma_fast_value > sma_slow_value
    )
    prev_stage_2 = bool(previous.get("is_stage_2", False))

    if is_stage_2 and not prev_stage_2:
        alerts.append("Entered Stage 2 alignment")
    elif not is_stage_2 and prev_stage_2:
        alerts.append("Exited Stage 2 alignment")

    if relative_strength > 0 and prev_relative_strength <= 0:
        alerts.append("RS breakout vs benchmark")
    elif relative_strength < 0 and prev_relative_strength >= 0:
        alerts.append("RS breakdown vs benchmark")

    if relative_volume >= relative_volume_threshold and prev_relative_volume < relative_volume_threshold:
        alerts.append(f"Volume spike: {relative_volume:.1f}x normal")

    if _crossed_above(close, prev_close, sma_slow_value, prev_sma_slow):
        alerts.append(f"Price crossed above SMA{sma_slow}")
    elif _crossed_below(close, prev_close, sma_slow_value, prev_sma_slow):
        alerts.append(f"Price crossed below SMA{sma_slow}")

    if _crossed_above(close, prev_close, sma_fast_value, prev_sma_fast):
        alerts.append(f"Price crossed above SMA{sma_fast}")
    elif _crossed_below(close, prev_close, sma_fast_value, prev_sma_fast):
        alerts.append(f"Price crossed below SMA{sma_fast}")

    if _crossed_above(monthly_rsi, prev_monthly_rsi, monthly_breakout, monthly_breakout):
        alerts.append(f"Monthly RSI crossed above {monthly_breakout:.0f}")

    if _crossed_below(weekly_rsi, prev_weekly_rsi, weekly_breakdown, weekly_breakdown):
        alerts.append(f"Weekly RSI crossed below {weekly_breakdown:.0f}")

    if close is not None and high_52w is not None and prev_close is not None and prev_high_52w is not None:
        if close >= high_52w and prev_close < prev_high_52w:
            alerts.append("New 52-week high")

    if close is not None and low_52w is not None and prev_close is not None and prev_low_52w is not None:
        if close <= low_52w and prev_close > prev_low_52w:
            alerts.append("New 52-week low")

    return alerts


def update_ticker_state(ticker, analyzed_data, current_full_state, config=None):
    """Persist the minimum state needed for transition detection."""
    if analyzed_data is None or analyzed_data.empty:
        return current_full_state

    latest = analyzed_data.iloc[-1]
    settings = _get_settings(config)

    close = _safe_float(latest.get("Close"), 0.0)
    sma50 = _safe_float(latest.get("SMA50"), 0.0)
    sma200 = _safe_float(latest.get("SMA200"), 0.0)
    rsi_weekly = _safe_float(latest.get("RSI_Weekly"), 50.0)
    rsi_monthly = _safe_float(latest.get("RSI_Monthly"), 50.0)
    mrs = _safe_float(latest.get("MRS"), 0.0)
    rv = _safe_float(latest.get("RV"), 1.0)
    high_52w = _safe_float(latest.get("High_52W"))
    low_52w = _safe_float(latest.get("Low_52W"))

    current_full_state[ticker] = {
        "close": close,
        "sma50": sma50,
        "sma200": sma200,
        "rsi_weekly": rsi_weekly,
        "rsi_monthly": rsi_monthly,
        "mrs": mrs,
        "rv": rv,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "is_stage_2": bool(close > sma50 > sma200 if pd.notna(close) and pd.notna(sma50) and pd.notna(sma200) else False),
        "sma_fast": int(settings["sma_fast"]),
        "sma_slow": int(settings["sma_slow"]),
    }

    return current_full_state
