import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def get_price_history(ticker, period="2y", interval="1d"):
    """
    Fetches historical price data from Yahoo Finance.
    
    Args:
        ticker (str): The stock symbol (e.g., 'AAPL', 'SPY').
        period (str): Amount of data to fetch. '2y' is recommended 
                     to provide enough data for 252-day (52W) metrics.
        interval (str): Data resolution. Default is '1d'.
        
    Returns:
        pd.DataFrame: Cleaned OHLCV data or None if fetching fails.
    """
    try:
        # 1. Initialize Ticker
        stock = yf.Ticker(ticker)
        
        # 2. Fetch Data
        # auto_adjust=True handles stock splits and dividends automatically
        df = stock.history(period=period, interval=interval, auto_adjust=True)
        
        # 3. Validation
        if df.empty:
            print(f"⚠️ Warning: No data found for ticker {ticker}. Check symbol.")
            return None
        
        # 4. Data Cleaning
        # Ensure the index is a DatetimeIndex (required for resample in indicators.py)
        df.index = pd.to_datetime(df.index)
        
        # Remove any potential timezone issues for easier plotting/alignment
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
            
        return df

    except Exception as e:
        print(f"❌ Error fetching data for {ticker}: {e}")
        return None

def get_latest_price(ticker):
    """
    Quick helper for real-time snapshots in Telegram alerts.
    """
    try:
        data = yf.download(ticker, period="1d", interval="1m", progress=False)
        if not data.empty:
            # Return the last available closing price
            return float(data['Close'].iloc[-1])
        return None
    except:
        return None
