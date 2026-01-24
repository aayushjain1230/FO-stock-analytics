import pandas as pd
import numpy as np

def generate_rating(df):
    """
    Final JFO Scoring Engine: Total Market Intelligence Integration.
    
    This engine combines:
    1. Stage Analysis (Weinstein Logic)
    2. Institutional Footprint (RV & RS)
    3. Momentum (Multi-Timeframe RSI)
    4. Volatility Guard (ATR Extension Penalty)
    """
    # Safety Check: Ensure data exists and critical trend indicators are present
    if df is None or df.empty or 'SMA200' not in df.columns:
        return {
            "score": 0, 
            "rating": "Data Error",
            "is_extended": False,
            "events": {},
            "metrics": {}
        }

    latest = df.iloc[-1]
    score = 0
    
    # --- 1. TREND & STAGE ANALYSIS (30 pts) ---
    # Weinstein Stage 2 Alignment: Price > SMA50 > SMA200
    is_stage_2 = False
    if latest['Close'] > latest['SMA50'] > latest['SMA200']:
        score += 30
        is_stage_2 = True
    elif latest['Close'] > latest['SMA200']:
        # Basic uptrend but not "perfect" alignment
        score += 15
    elif latest['Close'] > latest['SMA50']:
        # Minor recovery
        score += 5

    # --- 2. MULTI-TIMEFRAME MOMENTUM (20 pts) ---
    # RSI > 50 on Weekly/Monthly shows long-term buyers are in control
    rsi_w = latest.get('RSI_Weekly')
    rsi_m = latest.get('RSI_Monthly')
    
    if pd.notnull(rsi_m) and rsi_m > 50: 
        score += 10
    if pd.notnull(rsi_w) and rsi_w > 50: 
        score += 10

    # --- 3. INSTITUTIONAL VOLUME (20 pts) ---
    # Relative Volume (RV) captures big money footprints
    rv = latest.get('RV', 1.0)
    if rv >= 2.0:
        score += 20
    elif rv >= 1.5:
        score += 10
    
    # --- 4. RELATIVE STRENGTH DYNAMICS (30 pts) ---
    # MRS > 0 means the stock is outperforming the S&P 500
    mrs = latest.get('MRS', 0)
    rs_breakout = bool(latest.get('RS_Breakout'))
    
    if mrs > 0:
        score += 20
        # Fresh RS Breakout is highly bullish
        if rs_breakout:
            score += 10
    elif latest.get('RS_Line', 0) > latest.get('RS_SMA20', 0):
        score += 5

    # --- 5. THE VOLATILITY GUARD (Penalty Section) ---
    # Penalty for stocks that are "Vertical" or "Over-Extended" from the 20 SMA.
    # If Dist_SMA20 > 3.0 ATRs, the stock is high risk for a pullback.
    dist_sma20 = latest.get('Dist_SMA20', 0)
    is_extended = dist_sma20 > 3.0
    
    if is_extended:
        score -= 25 # Heavy penalty to drop the Tier status

    # --- FINAL RATING ASSIGNMENT ---
    if score >= 80: rating = "Tier 1: Market Leader 🏆"
    elif score >= 60: rating = "Tier 2: Improving 📈"
    elif score >= 40: rating = "Tier 3: Neutral ⚖️"
    elif score >= 20: rating = "Tier 4: Lagging 📉"
    else: rating = "Tier 5: Avoid 🔴"

    # --- 52-WEEK HIGH PROXIMITY ---
    # Finding "Blue Sky" breakout potential
    dist_high = "N/A"
    high_52w = df['Close'].rolling(window=252, min_periods=1).max().iloc[-1]
    if pd.notnull(high_52w) and high_52w != 0:
        percent_off = ((high_52w - latest['Close']) / high_52w) * 100
        dist_high = f"{round(percent_off, 2)}%"

    # Return summary for Telegram and main engine
    return {
        "score": max(0, score), # Ensure score doesn't go negative
        "rating": rating,
        "is_extended": is_extended,
        "events": {
            "golden_cross": bool(latest.get('Golden_Cross')),
            "volume_spike": bool(rv >= 2.0),
            "rs_breakout": rs_breakout,
            "stage_2": is_stage_2
        },
        "metrics": {
            "weekly_rsi": round(rsi_w, 2) if pd.notnull(rsi_w) else "N/A",
            "mrs_value": round(mrs, 2),
            "rel_volume": f"{round(rv, 2)}x",
            "dist_52w_high": dist_high,
            "volatility_risk": "HIGH" if is_extended else "NORMAL"
        }
    }
