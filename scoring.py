import pandas as pd

def generate_rating(df):
    """
    V5 Scoring Engine: Optimized for Institutional Footprint Detection.
    
    This engine uses Mansfield Relative Strength (MRS) and Relative Volume (RV)
    to identify Tier 1 Market Leaders.
    """
    # Safety Check: Ensure data exists and trend indicators are present
    if df is None or df.empty or 'SMA200' not in df.columns:
        return {"score": 0, "rating": "Data Error"}

    latest = df.iloc[-1]
    score = 0
    
    # --- 1. TREND ALIGNMENT (20 pts) ---
    # Non-negotiable for Stage 2 Uptrends
    if latest['Close'] > latest['SMA200']:
        score += 10
    if latest['Close'] > latest['SMA50']:
        score += 10

    # --- 2. MULTI-TIMEFRAME RSI (20 pts) ---
    # Checks if the stock is in a "Bullish Momentum" zone across weeks/months
    if pd.notnull(latest.get('RSI_Monthly')) and latest['RSI_Monthly'] > 50: 
        score += 10
    if pd.notnull(latest.get('RSI_Weekly')) and latest['RSI_Weekly'] > 50: 
        score += 10

    # --- 3. VOLUME & TREND EVENTS (30 pts) ---
    # RV (Relative Volume) > 2.0 indicates massive institutional interest
    rv = latest.get('RV', 1.0)
    if rv >= 2.0:
        score += 20
    elif rv >= 1.5:
        score += 10
    
    # Golden Cross adds long-term validity
    if latest.get('Golden_Cross') == True:
        score += 10

    # --- 4. RELATIVE STRENGTH DYNAMICS (30 pts) ---
    # MRS > 0 means the stock is bucking the market trend
    mrs = latest.get('MRS', 0)
    if mrs > 0:
        score += 20
        # Extra points for a fresh RS Breakout (crossing 0)
        if latest.get('RS_Breakout') == True:
            score += 10
    elif latest.get('RS_Line', 0) > latest.get('RS_SMA20', 0):
        # Consolidation points for minor outperformance
        score += 5

    # --- FINAL RATING ASSIGNMENT ---
    if score >= 80: rating = "Tier 1: Market Leader 🏆"
    elif score >= 60: rating = "Tier 2: Improving 📈"
    elif score >= 40: rating = "Tier 3: Neutral ⚖️"
    elif score >= 20: rating = "Tier 4: Lagging 📉"
    else: rating = "Tier 5: Avoid 🔴"

    # Distance from 52-Week High Calculation
    # Stocks near highs usually have the least "overhead supply" (easier to rise)
    dist_high = "N/A"
    high_52w = df['Close'].rolling(window=252).max().iloc[-1]
    if pd.notnull(high_52w) and high_52w != 0:
        percent_off = ((high_52w - latest['Close']) / high_52w) * 100
        dist_high = f"{round(percent_off, 2)}%"

    # Return summary for Telegram alerts and main logic
    return {
        "score": score, 
        "rating": rating,
        "events": {
            "golden_cross": bool(latest.get('Golden_Cross')),
            "volume_spike": bool(rv >= 2.0),
            "rs_breakout": bool(latest.get('RS_Breakout'))
        },
        "metrics": {
            "weekly_rsi": round(latest['RSI_Weekly'], 2) if pd.notnull(latest.get('RSI_Weekly')) else "N/A",
            "mrs_value": round(mrs, 2),
            "rel_volume": f"{round(rv, 2)}x",
            "dist_52w_high": dist_high
        }
    }
