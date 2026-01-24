import pandas as pd
import pandas_ta as ta
import numpy as np

def calculate_metrics(df, benchmark_df):
    """
    Advanced Technical Engine for the JFO Analytics Project.
    Integrates Institutional Volume, Relative Strength (MRS), and Multi-Timeframe RSI.
    """
    # Safety Check: Align dataframes to ensure dates match for RS calculations
    df, benchmark_df = df.align(benchmark_df, join='inner', axis=0)

    # 1. Trend Foundation
    df['SMA20'] = ta.sma(df['Close'], length=20)
    df['SMA50'] = ta.sma(df['Close'], length=50)
    df['SMA200'] = ta.sma(df['Close'], length=200)

    # 2. Institutional Volume Intelligence (CHANGE 1)
    # RV (Relative Volume) identifies the "Institutional Footprint"
    df['Vol_20_Avg'] = ta.sma(df['Volume'], length=20)
    df['RV'] = df['Volume'] / df['Vol_20_Avg']
    df['Volume_Spike'] = df['RV'] >= 2.0  # True if 2x average volume

    # 3. Mansfield Relative Strength - MRS (CHANGE 2)
    # Standard for professional Stage Analysis (Beating the S&P 500)
    df['RS_Line'] = df['Close'] / benchmark_df['Close']
    df['RS_SMA50'] = ta.sma(df['RS_Line'], length=50)
    # MRS > 0 means the stock is outperforming the benchmark relative to its own average
    df['MRS'] = ((df['RS_Line'] / df['RS_SMA50']) - 1) * 100
    df['RS_SMA20'] = ta.sma(df['RS_Line'], length=20) # Kept for scoring consistency

    # 4. Momentum (Daily RSI)
    df['RSI'] = ta.rsi(df['Close'], length=14)

    # 5. Multi-Timeframe Analysis (Weekly/Monthly RSI)
    ohlc_dict = {'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', 'Volume':'sum'}
    
    # Weekly Analysis
    df_weekly = df.resample('W').apply(ohlc_dict)
    df_weekly['RSI_W'] = ta.rsi(df_weekly['Close'], length=14)
    df['RSI_Weekly'] = df_weekly['RSI_W'].reindex(df.index, method='ffill')

    # Monthly Analysis
    df_monthly = df.resample('ME').apply(ohlc_dict)
    df_monthly['RSI_M'] = ta.rsi(df_monthly['Close'], length=14)
    df['RSI_Monthly'] = df_monthly['RSI_M'].reindex(df.index, method='ffill')

    # 6. Critical Signal Triggers
    # Golden Cross: Bullish long-term shift
    df['Golden_Cross'] = (df['SMA50'] > df['SMA200']) & (df['SMA50'].shift(1) <= df['SMA200'].shift(1))
    
    # RS Breakout: Stock beginning to lead the market
    # Logic: Current RS > Previous 20-day High of RS
    df['RS_Breakout'] = (df['MRS'] > 0) & (df['MRS'].shift(1) <= 0)

    return df

def calculate_market_leader_score(row):
    """
    JFO "Market Leader" Score (0-100).
    Weighted heavily toward Trend (40%), Relative Strength (30%), and Momentum (30%).
    """
    score = 0
    
    # Prevent calculation if critical data is missing
    if pd.isna(row['SMA200']) or pd.isna(row['RSI_Weekly']) or pd.isna(row['MRS']):
        return 0

    # Logic 1: Trend Health (40 pts)
    # Price above 200 SMA is the non-negotiable for "Market Leaders"
    if row['Close'] > row['SMA200']:
        score += 30
    if row['SMA50'] > row['SMA200']:
        score += 10
    
    # Logic 2: Relative Strength (30 pts)
    # MRS > 0 is the primary indicator of institutional accumulation vs SPY
    if row['MRS'] > 0:
        score += 20
    if row['RS_Line'] > row['RS_SMA20']:
        score += 10
    
    # Logic 3: Momentum & Volume Confirmation (30 pts)
    # Multi-timeframe RSI confirmation
    if row['RSI_Weekly'] > 50:
        score += 10
    if row['RSI_Monthly'] > 50:
        score += 10
    # Bonus for Volume Confirmation
    if row.get('RV', 0) > 1.5:
        score += 10
        
    # Cap score at 100
    return min(score, 100)
