🏦 JFO Market Intelligence Engine v2
An institutional-grade market scanner that identifies Tier 1 Market Leaders using the Mansfield Relative Strength (MRS) and Stan Weinstein Stage Analysis methodologies. It operates automatically via GitHub Actions and sends executive-level alerts directly to Telegram.

🚀 Key Features
Stage 2 Detection: Identifies the precise moment a stock enters a long-term bullish uptrend.

Relative Strength (MRS): Compares every stock against the S&P 500 (SPY) to find true outperformance.

Institutional Volume: Detects "Big Money" footprints by scanning for significant relative volume spikes.

Volatility Guard: A built-in safety mechanism that penalizes over-extended stocks to prevent "buying the top."

Smart Alerts: State-persistent memory ensures you only get notified of new technical events (no spam).

📁 Project Structure
main.py: The central controller that manages the workflow.

indicators.py: The "Brain"—calculates all technical math and Mansfield RS.

scoring.py: The "Judge"—ranks stocks from Tier 1 (Leader) to Tier 5 (Avoid).

state_manager.py: The "Memory"—tracks previous data to detect trend crossovers.

telegram_notifier.py: The "Messenger"—formats and dispatches alerts with TradingView links.

plotting.py: The "Artist"—generates high-resolution technical dashboards.

🛠 Setup & Installation
Clone the Repository:

Bash
git clone https://github.com/yourusername/jfo-engine.git
cd jfo-engine
Install Requirements:

Bash
pip install -r requirements.txt
Configure Credentials: Create a config/config.json file:

JSON
{
  "telegram": {
    "token": "YOUR_BOT_TOKEN",
    "chat_id": "YOUR_CHAT_ID"
  },
  "watchlist": ["AAPL", "NVDA", "TSLA"],
  "benchmark": "SPY"
}
🤖 Automation
The engine is configured to run via GitHub Actions every 30 minutes during market hours. It automatically checks if the NYSE is open before executing to save processing minutes.

⚖️ Disclaimer
This software is for educational and technical analysis purposes only. It does not constitute financial advice. Trading involves significant risk. Always perform your own due diligence before making investment decisions.
