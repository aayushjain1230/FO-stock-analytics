import pandas as pd

def generate_rating(df):
    """
    Final JFO Scoring Engine: Total Market Intelligence Integration.
    
    This engine combines:
    1. Stage Analysis (Weinstein Logic)
    2. Institutional Footprint (RV & RS)
    3. Momentum (Multi-Timeframe RSI)
    4. Volatility Guard (ATR Extension Penalty)
    5. Proximity to Annual Highs/Lows
    """

    # -----------------------------
    # SAFETY CHECKS
    # -----------------------------
    required_columns = {
        "Close", "SMA50", "SMA200"
    }

    if df is None or df.empty:
        return _data_error()

    if not required_columns.issubset(df.columns):
        return _data_error()

    latest = df.iloc[-1]
    score = 0
    is_stage_2 = False

    # -----------------------------
    # 1. TREND & STAGE ANALYSIS (30)
    # -----------------------------
    if latest["Close"] > latest["SMA50"] > latest["SMA200"]:
        score += 30
        is_stage_2 = True
    elif latest["Close"] > latest["SMA200"]:
        score += 15
    elif latest["Close"] > latest["SMA50"]:
        score += 5

    # -----------------------------
    # 2. MULTI-TIMEFRAME MOMENTUM (20)
    # -----------------------------
    rsi_w = latest.get("RSI_Weekly")
    rsi_m = latest.get("RSI_Monthly")

    if pd.notna(rsi_m) and rsi_m > 50:
        score += 10

    if pd.notna(rsi_w) and rsi_w > 50:
        score += 10

    # -----------------------------
    # 3. INSTITUTIONAL VOLUME (20)
    # -----------------------------
    rv = latest.get("RV", 1.0)

    if rv >= 2.0:
        score += 20
    elif rv >= 1.5:
        score += 10

    # -----------------------------
    # 4. RELATIVE STRENGTH (30)
    # -----------------------------
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

    # -----------------------------
    # 5. VOLATILITY GUARD (PENALTY)
    # -----------------------------
    dist_sma20 = latest.get("Dist_SMA20", 0)
    is_extended = dist_sma20 > 3.0

    if is_extended:
        score -= 25

    score = max(score, 0)

    # -----------------------------
    # FINAL RATING
    # -----------------------------
    if score >= 80:
        rating = "Tier 1: Market Leader 🏆"
    elif score >= 60:
        rating = "Tier 2: Improving 📈"
    elif score >= 40:
        rating = "Tier 3: Neutral ⚖️"
    elif score >= 20:
        rating = "Tier 4: Lagging 📉"
    else:
        rating = "Tier 5: Avoid 🔴"

    # -----------------------------
    # 52-WEEK HIGH/LOW PROXIMITY
    # -----------------------------
    dist_high = "N/A"
    dist_low = "N/A"

    if "Close" in df.columns:
        # Calculate Rolling Max/Min for 252 trading days (1 Year)
        rolling_window = df["Close"].rolling(window=252, min_periods=1)
        high_52w = rolling_window.max().iloc[-1]
        low_52w = rolling_window.min().iloc[-1]

        # Calculate Distance from 52-Week High
        if pd.notna(high_52w) and high_52w > 0:
            off_high = ((high_52w - latest["Close"]) / high_52w) * 100
            dist_high = f"{round(off_high, 2)}%"

        # Calculate Distance from 52-Week Low
        if pd.notna(low_52w) and low_52w > 0:
            off_low = ((latest["Close"] - low_52w) / low_52w) * 100
            dist_low = f"{round(off_low, 2)}%"

    # -----------------------------
    # RETURN PAYLOAD
    # -----------------------------
    return {
        "score": score,
        "rating": rating,
        "is_extended": is_extended,
        "events": {
            "golden_cross": bool(latest.get("Golden_Cross", False)),
            "volume_spike": rv >= 2.0,
            "rs_breakout": rs_breakout,
            "stage_2": is_stage_2,
        },
        "metrics": {
            "weekly_rsi": round(rsi_w, 2) if pd.notna(rsi_w) else "N/A",
            "mrs_value": round(mrs, 2),
            "rel_volume": f"{round(rv, 2)}x",
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
