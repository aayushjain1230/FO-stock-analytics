import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import math

def create_chart(ticker, df, benchmark_df):
    """
    Detailed single-stock chart with SMAs and Benchmark overlay.
    Useful for deep-dives into a specific ticker.
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True, 
                                   gridspec_kw={'height_ratios': [3, 1]})

    # --- TOP PANEL: PRICE, SMAs & BENCHMARK ---
    ax1.plot(df.index, df['Close'], color='black', label='Price', linewidth=1.5, zorder=5)
    ax1.plot(df.index, df['SMA20'], color='cyan', label='20-Day SMA', alpha=0.6, linestyle='--')
    ax1.plot(df.index, df['SMA50'], color='orange', label='50-Day SMA', alpha=0.8)
    ax1.plot(df.index, df['SMA200'], color='red', label='200-Day SMA', linewidth=1.5)
    
    # Overlay Normalized SPY Benchmark
    bench_norm = (benchmark_df['Close'] / benchmark_df['Close'].iloc[0]) * df['Close'].iloc[0]
    ax1.plot(df.index, bench_norm, color='gray', label='SPY (Bench)', alpha=0.3, linewidth=1)
    
    # 52-Week High Reference
    if '52W_High' in df.columns:
        ax1.axhline(y=df['52W_High'].iloc[-1], color='green', linestyle=':', alpha=0.5, label='52W High')

    ax1.set_title(f"{ticker} - Detailed Technical Analysis", fontsize=16, fontweight='bold')
    ax1.set_ylabel("Price (USD)")
    ax1.legend(loc='upper left', ncol=2, fontsize=9)
    ax1.grid(True, alpha=0.2)

    # --- BOTTOM PANEL: RELATIVE STRENGTH ---
    if 'RS_Line' in df.columns:
        ax2.plot(df.index, df['RS_Line'], color='purple', label='RS Line (Price/SPY)')
        ax2.plot(df.index, df['RS_SMA20'], color='gray', linestyle=':', alpha=0.6)
        ax2.fill_between(df.index, df['RS_Line'], df['RS_SMA20'], 
                         where=(df['RS_Line'] >= df['RS_SMA20']), color='green', alpha=0.1)
        
        ax2.set_ylabel("RS Ratio")
        ax2.legend(loc='upper left', fontsize=9)
        ax2.grid(True, alpha=0.2)

    plt.tight_layout()
    plt.show()

def create_comparison_chart(all_stock_data, benchmark_df, max_tickers=4):
    """
    Executive Dashboard - Professional Way:
    - Top: All stocks cumulative returns vs Benchmark %
    - Bottom: Individual blocks showing Price + SMAs vs Relative Strength
    """
    if not all_stock_data:
        print("No data available for comparison.")
        return

    tickers = list(all_stock_data.keys())[:max_tickers]
    num_stocks = len(tickers)
    
    # Grid Setup: 1 row for main chart, then 1 row per stock
    # Each stock row has 2 columns: [Price+SMAs] and [RS Line]
    fig = plt.figure(figsize=(15, 5 + (4 * num_stocks)))
    gs = fig.add_gridspec(num_stocks + 1, 2, height_ratios=[2] + [1.2]*num_stocks)
    
    # --- 1. TOP PANEL: CUMULATIVE RETURNS % (Full Width) ---
    ax_main = fig.add_subplot(gs[0, :])
    bench_returns = (benchmark_df['Close'] / benchmark_df['Close'].iloc[0] - 1) * 100
    ax_main.plot(bench_returns.index, bench_returns, color='black', linewidth=2.5, label='S&P 500 (SPY)', zorder=10)

    for ticker in tickers:
        df = all_stock_data[ticker]
        stock_returns = (df['Close'] / df['Close'].iloc[0] - 1) * 100
        ax_main.plot(stock_returns.index, stock_returns, label=f'{ticker}', alpha=0.7)

    ax_main.set_title("Watchlist Performance vs Benchmark (Cumulative %)", fontsize=16, fontweight='bold')
    ax_main.set_ylabel("Return %")
    ax_main.legend(loc='upper left', ncol=3, fontsize=10)
    ax_main.grid(True, alpha=0.3)

    # --- 2. INDIVIDUAL ANALYSIS BLOCKS ---
    for i, ticker in enumerate(tickers):
        df = all_stock_data[ticker]
        row_idx = i + 1
        
        # COLUMN A: Price and SMAs
        ax_price = fig.add_subplot(gs[row_idx, 0])
        ax_price.plot(df.index, df['Close'], color='black', linewidth=1, label='Price')
        ax_price.plot(df.index, df['SMA20'], color='cyan', alpha=0.5, label='20 SMA', linestyle='--')
        ax_price.plot(df.index, df['SMA50'], color='orange', alpha=0.7, label='50 SMA')
        ax_price.plot(df.index, df['SMA200'], color='red', linewidth=1.2, label='200 SMA')
        
        ax_price.set_title(f"{ticker}: Price Trend (SMAs)", fontsize=11, fontweight='bold')
        ax_price.legend(fontsize=8, loc='best')
        ax_price.grid(True, alpha=0.1)
        ax_price.tick_params(axis='x', rotation=20, labelsize=8)

        # COLUMN B: Relative Strength
        ax_rs = fig.add_subplot(gs[row_idx, 1])
        if 'RS_Line' in df.columns:
            ax_rs.plot(df.index, df['RS_Line'], color='purple', label='RS Line')
            ax_rs.plot(df.index, df['RS_SMA20'], color='gray', linestyle='--', alpha=0.6)
            ax_rs.fill_between(df.index, df['RS_Line'], df['RS_SMA20'], 
                               where=(df['RS_Line'] >= df['RS_SMA20']), color='green', alpha=0.1)
            
            ax_rs.set_title(f"{ticker}: RS vs SPY", fontsize=11, fontweight='bold')
            ax_rs.grid(True, alpha=0.1)
            ax_rs.tick_params(axis='x', rotation=20, labelsize=8)

    plt.tight_layout()

    plt.show()
