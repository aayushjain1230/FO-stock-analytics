import pandas as pd

def generate_rating(df):
    """
    V2 Scoring Engine: Weights Weekly/Monthly RSI and Relative Strength.
    """
    if df.empty or 'SMA200' not in df.columns:
        return {"score": 0, "rating": "Data Error"}

    latest = df.iloc[-1]
    score = 0
    
    # --- LONG-TERM TREND (30 pts) ---
    if latest['Close'] > latest['SMA200']:
        score += 30

    # --- MULTI-TIMEFRAME RSI (25 pts) ---
    # Points for staying above the "Bullish Support" level of 40
    if latest['RSI_Monthly'] > 40: score += 15
    if latest['RSI_Weekly'] > 40: score += 10

    # --- RELATIVE STRENGTH (25 pts) ---
    if latest.get('RS_Trend') == True:
        score += 25

    # --- PRICE MOMENTUM (20 pts) ---
    if latest['Close'] > latest['SMA50']: score += 15
    # Bonus points for proximity to 52-week highs (Phase 5)
    if latest['Close'] >= (latest['52W_High'] * 0.98):
        score += 5

    # FINAL RATING ASSIGNMENT
    if score >= 85: rating = "Tier 1: Market Leader"
    elif score >= 65: rating = "Tier 2: Improving"
    elif score >= 45: rating = "Tier 3: Neutral"
    elif score >= 25: rating = "Tier 4: Lagging"
    else: rating = "Tier 5: Avoid"

    # Return summary for alerts and display
    return {
        "score": score, 
        "rating": rating,
        "metrics": {
            # Added pd.notnull check to prevent rounding errors on new/thin data
            "weekly_rsi": round(latest['RSI_Weekly'], 2) if pd.notnull(latest['RSI_Weekly']) else "N/A",
            "rs_trend": "Positive" if latest.get('RS_Trend') else "Negative",
            "dist_52w_high": f"{round(((latest['52W_High'] - latest['Close']) / latest['52W_High']) * 100, 2)}%" if pd.notnull(latest['52W_High']) else "N/A"
        }
    }