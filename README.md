JFO Technical Analytics Engine (Version 2.0)
A systematic quantitative platform designed for the Jain Family Office to identify high-probability trading setups by filtering the market for Relative Strength and Trend Alignment.

🚀 Overview
Version 2.0 transforms the project from a simple data scraper into a stateful, event-driven analytical platform. The engine automates the "selection" process by confirming momentum across multiple timeframes and benchmarking every ticker against the S&P 500.

🛠 Key Features
1. Institutional-Grade Indicators
Trend Stack: Uses 20, 50, and 200-day Simple Moving Averages (SMAs) to identify "Stage 2" uptrends.

Multi-Timeframe RSI: Incorporates both Weekly and Monthly RSI to ensure momentum is not just a short-term fluke.

Relative Strength (RS): A dedicated calculation of Price vs. SPY to find stocks with true "Alpha."

2. Intelligent Memory (State Management)
The engine utilizes a state.json persistence layer. It doesn't just look at today's data; it compares it to the previous run to detect:

SMA Crossovers: Alerts when price breaks above/below the 50 or 200 SMA.

Momentum Shifts: Alerts when RSI reclaims the 40-level (Bullish Shift).

Breakouts: Tracks new 52-week Highs and Lows automatically.

3. Professional Reporting
Automated Telegram Bot: Bundles all findings into a single, clean Markdown message.

Executive Snapshots: Every alert includes a snapshot of current Price, RSI, and RS Status.

Dynamic Dashboard: Generates a dual-axis visualization separating Price Action from Relative Strength.

📂 Project Structure
main.py: The central orchestrator and entry point.

indicators.py: Financial logic and technical calculations.

state_manager.py: The memory bank for crossover and event detection.

telegram_notifier.py: Formats and sends executive-friendly reports.

plotting.py: Generates the professional visualization dashboard.

config/config.json: Centralized settings for tickers, tokens, and thresholds.

🚦 Quick Start
Install Requirements:

Bash

pip install -r requirements.txt
Configure: Update config/config.json with your Telegram Bot Token and Chat ID.

Run the Engine:

Bash

python main.py
📈 Technical Logic: The "Market Leader" Score
The engine scores stocks on a 0-100 scale based on the following weights:

Price > SMA200: (Critical) Confirms the long-term bull market.

Relative Strength > RS SMA20: Confirms the stock is beating the S&P 500.

RSI Alignment: Confirms Weekly and Monthly momentum are both above 50.

🔮 Future Roadmap (V3)
Volume Profile Integration: Identifying institutional accumulation zones.

Sector Rotation: Grouping tickers to find industry leadership.

Backtesting: Verifying the historical win rate of the "Tier 1" scoring logic.

Developed for the Jain Family Office Precision. Performance. Persistence.