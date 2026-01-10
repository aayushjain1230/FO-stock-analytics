import pandas as pd

def generate_rating(df):
    """
    V3 Scoring Engine: Now rewards specific Technical Events like 
    Golden Crosses, RS Breakouts, and Volume Spikes.
    """
    if df.empty or 'SMA200' not in df.columns:
        return {"score": 0, "rating": "Data Error"}

    latest = df.iloc[-1]
    score = 0
    
    # --- 1. LONG-TERM TREND (25 pts) ---
    if latest['Close'] > latest['SMA200']:
        score += 25

    # --- 2. MULTI-TIMEFRAME MOMENTUM (20 pts) ---
    if latest['RSI_Monthly'] > 40: score += 10
    if latest['RSI_Weekly'] > 40: score += 10

    # --- 3. RELATIVE STRENGTH (25 pts) ---
    # Points for a general RS uptrend
    if latest.get('RS_Line', 0) > latest.get('RS_SMA20', 0):
        score += 15
    # BIG Bonus for a fresh RS Breakout happening TODAY
    if latest.get('RS_Breakout') == True:
        score += 10

    # --- 4. PRICE & VOLUME EVENTS (30 pts) ---
    # Trend alignment
    if latest['Close'] > latest['SMA50']: score += 10
    
    # Bonus for Golden Cross (Major long-term signal)
    if latest.get('Golden_Cross') == True:
        score += 10
        
    # Bonus for Volume Spike (Institutional buying)
    if latest.get('Volume_Spike') == True:
        score += 10

    # FINAL RATING ASSIGNMENT
    if score >= 85: rating = "Tier 1: Market Leader"
    elif score >= 65: rating = "Tier 2: Improving"
    elif score >= 45: rating = "Tier 3: Neutral"
    elif score >= 25: rating = "Tier 4: Lagging"
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
