import pandas as pd

def generate_rating(df):
    """
    V4 Scoring Engine: Highly sensitive to early momentum shifts, 
    volume spikes, and relative strength breakouts.
    
    This function analyzes the latest data point to assign a Tier-based 
    rating for the Jain Family Office reports.
    """
    # Safety Check: Ensure data exists and trend indicators are present
    if df is None or df.empty or 'SMA200' not in df.columns:
        return {"score": 0, "rating": "Data Error"}

    latest = df.iloc[-1]
    score = 0
    
    # --- 1. TREND ALIGNMENT (20 pts) ---
    # Rewards being on the 'right side' of the trend
    if latest['Close'] > latest['SMA200']:
        score += 10
    if latest['Close'] > latest['SMA50']:
        score += 10

    # --- 2. MULTI-TIMEFRAME RSI (20 pts) ---
    # Monthly/Weekly RSI > 40 indicates the 'Bullish Zone'
    if pd.notnull(latest.get('RSI_Monthly')) and latest['RSI_Monthly'] > 40: 
        score += 10
    if pd.notnull(latest.get('RSI_Weekly')) and latest['RSI_Weekly'] > 40: 
        score += 10

    # --- 3. SENSITIVE MOMENTUM & VOLUME (35 pts) ---
    # Major Points for Volume Spike (Detecting Institutional Accumulation)
    if latest.get('Volume_Spike') == True:
        score += 20 
    
    # Points for Golden Cross (SMA50 crossing SMA200)
    if latest.get('Golden_Cross') == True:
        score += 15

    # --- 4. RELATIVE STRENGTH DYNAMICS (25 pts) ---
    # Awarded if RS is currently 1% above its average (Sensitive Trigger)
    if latest.get('RS_Breakout') == True:
        score += 15
    # Baseline points for being above the RS average at all
    elif latest.get('RS_Line', 0) > latest.get('RS_SMA20', 0):
        score += 10

    # --- FINAL RATING ASSIGNMENT ---
    if score >= 80: rating = "Tier 1: Market Leader"
    elif score >= 60: rating = "Tier 2: Improving"
    elif score >= 40: rating = "Tier 3: Neutral"
    elif score >= 20: rating = "Tier 4: Lagging"
    else: rating = "Tier 5: Avoid"

    # Distance from 52-Week High Calculation
    dist_high = "N/A"
    if pd.notnull(latest.get('52W_High')) and latest['52W_High'] != 0:
        percent_off = ((latest['52W_High'] - latest['Close']) / latest['52W_High']) * 100
        dist_high = f"{round(percent_off, 2)}%"

    # Return summary for Telegram alerts and main logic
    return {
        "score": score, 
        "rating": rating,
        "events": {
            "golden_cross": bool(latest.get('Golden_Cross')),
            "volume_spike": bool(latest.get('Volume_Spike')),
            "rs_breakout": bool(latest.get('RS_Breakout'))
        },
        "metrics": {
            "weekly_rsi": round(latest['RSI_Weekly'], 2) if pd.notnull(latest.get('RSI_Weekly')) else "N/A",
            "rs_status": "Outperforming" if latest.get('RS_Breakout') else "Neutral",
            "dist_52w_high": dist_high
        }
    }
