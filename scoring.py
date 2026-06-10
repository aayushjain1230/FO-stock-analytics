import pandas as pd


DEFAULT_SETTINGS = {
    "rsi_leader_threshold": 50,
    "relative_volume_alert_threshold": 2.0,
    "relative_volume_watch_threshold": 1.5,
    "atr_extension_threshold": 3.0,
}


def _get_settings(config):
    settings = dict(DEFAULT_SETTINGS)
    if isinstance(config, dict):
        settings.update(config.get("settings", {}))
    return settings


def generate_rating(df, config=None):
    """
    Produce a rating payload for a single analyzed ticker.
    """
    required_columns = {"Close", "SMA50", "SMA200"}

    if df is None or df.empty or not required_columns.issubset(df.columns):
        return _data_error()

    settings = _get_settings(config)
    leader_rsi = float(settings["rsi_leader_threshold"])
    volume_alert = float(settings["relative_volume_alert_threshold"])
    volume_watch = float(settings["relative_volume_watch_threshold"])
    atr_extension = float(settings["atr_extension_threshold"])

    latest = df.iloc[-1]
    score = 0
    is_stage_2 = False

    if latest["Close"] > latest["SMA50"] > latest["SMA200"]:
        score += 30
        is_stage_2 = True
    elif latest["Close"] > latest["SMA200"]:
        score += 15
    elif latest["Close"] > latest["SMA50"]:
        score += 5

    weekly_rsi = latest.get("RSI_Weekly")
    monthly_rsi = latest.get("RSI_Monthly")
    if pd.notna(monthly_rsi) and monthly_rsi > leader_rsi:
        score += 10
    if pd.notna(weekly_rsi) and weekly_rsi > leader_rsi:
        score += 10

    relative_volume = latest.get("RV", 1.0)
    if relative_volume >= volume_alert:
        score += 20
    elif relative_volume >= volume_watch:
        score += 10

    mrs = latest.get("MRS", 0)
    rs_breakout = bool(latest.get("RS_Breakout", False))
    if mrs > 0:
        score += 20
        if rs_breakout:
            score += 10
    else:
        rs_line = latest.get("RS_Line", 0)
        rs_sma20 = latest.get("RS_SMA20", 0)
        if rs_line > rs_sma20:
            score += 5

    dist_sma20 = latest.get("Dist_SMA20", 0)
    is_extended = dist_sma20 > atr_extension
    if is_extended:
        score -= 25

    score = max(score, 0)

    if score >= 80:
        rating = "Tier 1: Market Leader"
    elif score >= 60:
        rating = "Tier 2: Improving"
    elif score >= 40:
        rating = "Tier 3: Neutral"
    elif score >= 20:
        rating = "Tier 4: Lagging"
    else:
        rating = "Tier 5: Avoid"

    dist_high = "N/A"
    dist_low = "N/A"
    high_52w = latest.get("High_52W")
    low_52w = latest.get("Low_52W")

    if pd.notna(high_52w) and high_52w > 0:
        dist_high = f"{round(((high_52w - latest['Close']) / high_52w) * 100, 2)}%"
    if pd.notna(low_52w) and low_52w > 0:
        dist_low = f"{round(((latest['Close'] - low_52w) / low_52w) * 100, 2)}%"

    return {
        "score": score,
        "rating": rating,
        "is_extended": is_extended,
        "events": {
            "golden_cross": bool(latest.get("Golden_Cross", False)),
            "volume_spike": relative_volume >= volume_alert,
            "rs_breakout": rs_breakout,
            "stage_2": is_stage_2,
        },
        "metrics": {
            "close": round(float(latest.get("Close", 0)), 2),
            "weekly_rsi": round(float(weekly_rsi), 2) if pd.notna(weekly_rsi) else "N/A",
            "monthly_rsi": round(float(monthly_rsi), 2) if pd.notna(monthly_rsi) else "N/A",
            "mrs_value": round(float(mrs), 2),
            "rel_volume": f"{round(float(relative_volume), 2)}x",
            "dist_52w_high": dist_high,
            "dist_52w_low": dist_low,
            "volatility_risk": "HIGH" if is_extended else "NORMAL",
        },
    }


def _data_error():
    return {
        "score": 0,
        "rating": "Data Error",
        "is_extended": False,
        "events": {},
        "metrics": {},
    }
