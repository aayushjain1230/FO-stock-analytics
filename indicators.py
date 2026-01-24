import pandas as pd
import pandas_ta as ta

def calculate_metrics(df, benchmark_df):
    """
    Enhanced technical indicators for the JFO Analytics Engine.
    Ensures alignment between ticker data and benchmark (SPY).
    """
    # Safety Check: Ensure dataframes are aligned by date
    df, benchmark_df = df.align(benchmark_df, join='inner', axis=0)

    # 1. Trend Indicators (Using pandas_ta for consistency)
    df['SMA20'] = ta.sma(df['Close'], length=20)
    df['SMA50'] = ta.sma(df['Close'], length=50)
    df['SMA200'] = ta.sma(df['Close'], length=200)

    # 2. RSI (Daily)
    df['RSI'] = ta.rsi(df['Close'], length=14)

    # 3. 52-Week High Reference
    df['52W_High'] = df['Close'].rolling(window=252, min_periods=1).max()

    # 4. Relative Strength (RS) vs Benchmark
    # Calculation: Price Ratio vs Moving Average of Ratio
    df['RS_Line'] = df['Close'] / benchmark_df['Close']
    df['RS_SMA20'] = ta.sma(df['RS_Line'], length=20)

    # 5. Multi-Timeframe Analysis
    ohlc_dict = {'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', 'Volume':'sum'}
    
    # Weekly RSI
    df_weekly = df.resample('W').apply(ohlc_dict)
    df_weekly['RSI_W'] = ta.rsi(df_weekly['Close'], length=14)
    df['RSI_Weekly'] = df_weekly['RSI_W'].reindex(df.index, method='ffill')

    # Monthly RSI
    df_monthly = df.resample('ME').apply(ohlc_dict)
    df_monthly['RSI_M'] = ta.rsi(df_monthly['Close'], length=14)
    df['RSI_Monthly'] = df_monthly['RSI_M'].reindex(df.index, method='ffill')

    # --- SENSITIVE EVENT TRIGGERS ---
    
    # 6. Trend Event: Golden/Death Cross
    df['Golden_Cross'] = (df['SMA50'] > df['SMA200']) & (df['SMA50'].shift(1) <= df['SMA200'].shift(1))
    df['Death_Cross'] = (df['SMA50'] < df['SMA200']) & (df['SMA50'].shift(1) >= df['SMA200'].shift(1))

    # 7. RS Breakout (1% threshold for sensitivity)
    df['RS_Breakout'] = (df['RS_Line'] > (df['RS_SMA20'] * 1.01))

    # 8. Volume Spike (1.5x of 20-day Average)
    df['Vol_20_Avg'] = ta.sma(df['Volume'], length=20)
    df['Volume_Spike'] = df['Volume'] > (df['Vol_20_Avg'] * 1.5)

    return df

def calculate_market_leader_score(row):
    """
    Quantifies the strength of a ticker on a 0-100 scale.
    Designed for the JFO "Market Leader" scoring logic.
    """
    score = 0
    
    # Check for NaN to avoid logic errors
    if pd.isna(row['SMA200']) or pd.isna(row['RSI_Weekly']):
        return 0

    # Logic 1: Price > SMA200 (Foundation of Stage 2 Uptrend) - 40 pts
    if row['Close'] > row['SMA200']:
        score += 40
    
    # Logic 2: RS Line > RS SMA20 (Beating the S&P 500) - 30 pts
    if row['RS_Line'] > row['RS_SMA20']:
        score += 30
    
    # Logic 3: RSI Alignment (Weekly/Monthly > 50) - 15 pts each
    if row['RSI_Weekly'] > 50:
        score += 15
    if row['RSI_Monthly'] > 50:
        score += 15
        
    return score
