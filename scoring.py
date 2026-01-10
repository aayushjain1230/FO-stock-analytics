import pandas as pd

def generate_rating(df):
    """
    V4 Scoring Engine: Highly sensitive to early momentum shifts, 
    volume spikes, and relative strength breakouts.
    """
    if df.empty or 'SMA200' not in df.columns:
        return {"score": 0, "rating": "Data Error"}

    latest = df.iloc[-1]
    score = 0
    
    # --- 1. TREND ALIGNMENT (20 pts) ---
    if latest['Close'] > latest['SMA200']:
        score += 10
    if latest['Close'] > latest['SMA50']:
        score += 10

    # --- 2. MULTI-TIMEFRAME RSI (20 pts) ---
    if latest['RSI_Monthly'] > 40: score += 10
    if latest['RSI_Weekly'] > 40: score += 10

    # --- 3. SENSITIVE MOMENTUM & VOLUME (35 pts) ---
    # Major Points for Volume Spike (Institutional buying)
    if latest.get('Volume_Spike') == True:
        score += 20 
    
    # Points for Golden Cross
    if latest.get('Golden_Cross') == True:
        score += 15

    # --- 4. RELATIVE STRENGTH DYNAMICS (25 pts) ---
    # Awarded if RS is currently 1% above its average (Sensitive Trigger)
    if latest.get('RS_Breakout') == True:
        score += 15
    # Baseline points for being above the RS average at all
    elif latest.get('RS_Line', 0) > latest.get('RS_SMA20', 0):
        score += 10

    # FINAL RATING ASSIGNMENT
    if score >= 80: rating = "Tier 1: Market Leader"
    elif score >= 60: rating = "Tier 2: Improving"
    elif score >= 40: rating = "Tier 3: Neutral"
    elif score >= 20: rating = "Tier 4: Lagging"
    else: rating = "Tier 5: Avoid"

    # Return summary for Telegram alerts
    return {
        "score": score, 
        "rating": rating,
        "events": {
            "golden_cross": bool(latest.get('Golden_Cross')),
            "volume_spike": bool(latest.get('Volume_Spike')),
            "rs_breakout": bool(latest.get('RS_Breakout'))
        },
        "metrics": {
            "weekly_rsi": round(latest['RSI_Weekly'], 2) if pd.notnull(latest['RSI_Weekly']) else "N/A",
            "rs_status": "Outperforming" if latest.get('RS_Breakout') else "Neutral",
            "dist_52w_high": f"{round(((latest['52W_High'] - latest['Close']) / latest['52W_High']) * 100, 2)}%" if pd.notnull(latest['52W_High']) else "N/A"
        }
    }
