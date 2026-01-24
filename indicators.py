import pandas as pd
import pandas_ta as ta
import numpy as np

def calculate_metrics(df, benchmark_df):
    """
    Advanced Technical Engine for the JFO Analytics Project.
    Integrates Institutional Volume, Mansfield RS, ATR Volatility, and Multi-Timeframe RSI.
    """
    # Safety Check: Align dataframes to ensure dates match for RS calculations
    df, benchmark_df = df.align(benchmark_df, join='inner', axis=0)

    # 1. Trend Foundation & Stage Analysis
    df['SMA20'] = ta.sma(df['Close'], length=20)
    df['SMA50'] = ta.sma(df['Close'], length=50)
    df['SMA200'] = ta.sma(df['Close'], length=200)

    # 2. Institutional Volume Intelligence
    # RV (Relative Volume) identifies the "Institutional Footprint"
    df['Vol_20_Avg'] = ta.sma(df['Volume'], length=20)
    df['RV'] = df['Volume'] / df['Vol_20_Avg']
    df['Volume_Spike'] = df['RV'] >= 2.0  # True if 2x average volume

    # 3. Mansfield Relative Strength - MRS 
    # Standard for professional Stage Analysis (Beating the S&P 500)
    df['RS_Line'] = df['Close'] / benchmark_df['Close']
    df['RS_SMA50'] = ta.sma(df['RS_Line'], length=50)
    # MRS > 0 means the stock is outperforming the benchmark relative to its own average
    df['MRS'] = ((df['RS_Line'] / df['RS_SMA50']) - 1) * 100
    df['RS_SMA20'] = ta.sma(df['RS_Line'], length=20) 

    # 4. Volatility Guard (ATR)
    # Used to detect if a stock is "extended" or over-volatile
    df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
    # Distance from 20SMA in ATR units (Standardized Volatility Score)
    df['Dist_SMA20'] = (df['Close'] - df['SMA20']) / df['ATR']

    # 5. Momentum (Daily RSI)
    df['RSI'] = ta.rsi(df['Close'], length=14)

    # 6. Multi-Timeframe Analysis (Weekly/Monthly RSI)
    ohlc_dict = {'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', 'Volume':'sum'}
    
    # Weekly Analysis
    df_weekly = df.resample('W').apply(ohlc_dict)
    df_weekly['RSI_W'] = ta.rsi(df_weekly['Close'], length=14)
    df['RSI_Weekly'] = df_weekly['RSI_W'].reindex(df.index, method='ffill')

    # Monthly Analysis
    df_monthly = df.resample('ME').apply(ohlc_dict)
    df_monthly['RSI_M'] = ta.rsi(df_monthly['Close'], length=14)
    df['RSI_Monthly'] = df_monthly['RSI_M'].reindex(df.index, method='ffill')

    # 7. Critical Signal Triggers
    # Golden Cross: Bullish long-term shift
    df['Golden_Cross'] = (df['SMA50'] > df['SMA200']) & (df['SMA50'].shift(1) <= df['SMA200'].shift(1))
    
    # RS Breakout: Stock beginning to lead the market (MRS crosses 0)
    df['RS_Breakout'] = (df['MRS'] > 0) & (df['MRS'].shift(1) <= 0)

    # 8. Market Regime Status (Appended to DataFrame metadata)
    df.attrs['market_regime'] = get_market_regime_label(benchmark_df)

    return df

def get_market_regime_label(spy_df):
    try:
        if spy_df is None or len(spy_df) < 200:
            return "Unknown (Incomplete Data)"

        # Calculate SMA200
        sma_series = ta.sma(spy_df['Close'], length=200)
        
        if sma_series is None or sma_series.isna().all():
            return "Neutral (Calculating...)"

        latest_close = spy_df['Close'].iloc[-1]
        latest_sma = sma_series.iloc[-1]

        if latest_close > latest_sma:
            return "🟢 Bullish (Above SMA200)"
        else:
            return "🔴 Bearish (Below SMA200)"
    except Exception as e:
        print(f"Regime Check Error: {e}")
        return "Neutral"

def calculate_market_leader_score(row):
    """
    JFO "Market Leader" Score (0-100).
    Weighted by Stage (40%), Relative Strength (30%), and Momentum (30%).
    Includes Volatility Penalty for "Climax" runs.
    """
    score = 0
    
    # Safety Check
    if pd.isna(row['SMA200']) or pd.isna(row['RSI_Weekly']) or pd.isna(row['MRS']):
        return 0

    # Logic 1: Stage & Trend Health (40 pts)
    # Stage 2 Setup: Price > SMA50 > SMA200
    is_stage_2 = row['Close'] > row['SMA50'] > row['SMA200']
    if is_stage_2:
        score += 40
    elif row['Close'] > row['SMA200']:
        score += 20
    
    # Logic 2: Relative Strength (30 pts)
    if row['MRS'] > 0:
        score += 20
    if row['RS_Line'] > row['RS_SMA20']:
        score += 10
    
    # Logic 3: Momentum & Volume (30 pts)
    if row['RSI_Weekly'] > 50:
        score += 10
    if row['RSI_Monthly'] > 50:
        score += 10
    if row.get('RV', 0) > 1.5:
        score += 10
        
    # Logic 4: Extension Penalty (The "Climax" Run Filter)
    # Penalty if price is too far (over 3 ATRs) from its 20-day average
    if row.get('Dist_SMA20', 0) > 3.0:
        score -= 20
        
    return max(0, min(score, 100))

