# 📈 Jain Family Office: Market Intelligence Engine

An automated institutional-grade market scanner and alerting system. This engine monitors your personal watchlist and the S&P 500 sectors, generating technical analysis reports and visual dashboards directly to Telegram.

---

## 🚀 Key Features

* **Dual-Layer Scanning:** Tracks your custom `watchlist.json` + scans the entire S&P 500 for momentum leaders and laggards.
* **Smart Alerts:** Only notifies you via Telegram when data actually changes (MD5 Fingerprinting).
* **Executive Dashboards:** Generates high-resolution multi-panel charts of your entire watchlist vs. the S&P 500 benchmark.
* **Market Gatekeeper:** Automatically respects NYSE market hours and holidays to save on compute minutes.
* **Structured Logging:** Comprehensive logging system with file and console outputs.
* **Error Handling:** Robust retry logic and error recovery for network operations.
* **Caching:** Intelligent caching to reduce API calls and improve performance.
* **GitHub Actions Ready:** Fully automated via cron schedules—no server required.

---

## 📂 Project Structure

| File / Folder | Purpose |
| :--- | :--- |
| `main.py` | The central brain; handles logic, orchestration, and scheduling. |
| `watchlist.json` | Persistent list of your favorite stock tickers. |
| `plots/` | Stores generated PNG dashboards and technical charts. |
| `state/` | Stores historical data and report hashes for deduplication. |
| `logs/` | Daily log files with detailed execution history. |
| `cache/` | Cached data (S&P 500 sector list, etc.) to reduce API calls. |
| `indicators.py` | TA logic (RSI, Moving Averages, RS Line, Mansfield RS). |
| `telegram_notifier.py` | Handles message bundling and delivery to Telegram. |
| `scoring.py` | Multi-factor scoring engine for stock ranking. |
| `state_manager.py` | Manages state transitions and alert detection. |
| `plotting.py` | Generates visualization dashboards. |
| `logger_config.py` | Centralized logging configuration. |
| `utils.py` | Utility functions (retry logic, caching, validation). |
| `config/config.json` | Configuration file (benchmark, settings). |

---

## 🛠 Setup & Installation

### 1. Prerequisites
Ensure you have Python 3.9+ installed.

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configuration

#### Option A: Environment Variables (Recommended for Local)
Create a `.env` file in the project root:
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```env
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
LOG_LEVEL=INFO
```

#### Option B: Config File
Edit `config/config.json`:
```json
{
  "benchmark": "SPY",
  "telegram": {
    "token": "your_token",
    "chat_id": "your_chat_id"
  }
}
```

#### Option C: GitHub Secrets (For GitHub Actions)
Your workflow is already configured to use GitHub Secrets. Just add them in repository settings:

1. Go to **Settings** → **Secrets and variables** → **Actions**
2. Click **"New repository secret"**
3. Add these two secrets:
   - `TELEGRAM_BOT_TOKEN` - Your bot token from [@BotFather](https://t.me/botfather)
   - `TELEGRAM_CHAT_ID` - Your chat ID from [@userinfobot](https://t.me/userinfobot)

**Priority Order:**
1. ✅ **Environment Variables** (GitHub Secrets in Actions, `.env` locally) - **HIGHEST PRIORITY**
2. ⚙️ Config file (`config/config.json`) - Fallback
3. ❌ Error if neither is set

### 4. Get Telegram Credentials

1. Create a bot via [@BotFather](https://t.me/botfather) on Telegram
2. Get your bot token
3. Get your chat ID by messaging [@userinfobot](https://t.me/userinfobot)

### 5. Set Up Watchlist

Edit `watchlist.json` or use CLI:
```bash
python main.py --add AAPL MSFT GOOGL
python main.py --list
```

---

## 🎯 Usage

### Basic Analysis
```bash
python main.py
# or explicitly
python main.py --analyze
```

### Watchlist Management
```bash
# Add tickers
python main.py --add AAPL TSLA NVDA

# Remove tickers
python main.py --remove TSLA

# List current watchlist
python main.py --list
```

### Manual Ticker Override
```bash
# Scan only specific tickers (useful for testing)
export MANUAL_TICKERS=AAPL,MSFT,GOOGL
python main.py --analyze
```

---

## 📊 How It Works

1. **Data Acquisition**: Downloads 1 year of daily data for all tickers
2. **Technical Analysis**: Calculates indicators (SMA, RSI, Mansfield RS, Volume)
3. **Scoring**: Multi-factor scoring system (0-100) based on:
   - Stage Analysis (Weinstein methodology)
   - Momentum (Multi-timeframe RSI)
   - Institutional Volume
   - Relative Strength vs. S&P 500
4. **Alert Detection**: Compares current state with previous to detect transitions
5. **Report Generation**: Creates executive-friendly reports with TradingView links
6. **Deduplication**: Only sends Telegram messages when content actually changes
7. **Visualization**: Generates comparison charts for watchlist tickers

---

## ⏰ S&P 500 Scanning Schedule

### When Does S&P 500 Run?

**Answer: Every time the script runs** (which is every 30 minutes during market hours)

**Current Behavior:**
- The script scans **BOTH** your watchlist **AND** all S&P 500 tickers every single run
- This means it combines your watchlist + ~500 S&P 500 stocks = ~500+ tickers scanned each run

**GitHub Actions Schedule:**
- Runs every **30 minutes** during market hours
- Schedule: `*/30 13-21 * * 1-5` (13:00-21:00 UTC, Monday-Friday)
- That's **~16 runs per day** during market hours
- Each run scans all S&P 500 stocks

**What Gets Reported:**
- **Watchlist tickers**: Always included in reports (if they have alerts)
- **S&P 500 Leaders**: Only stocks with score ≥ 85 (Tier 1)
- **S&P 500 Laggards**: Only stocks with score ≤ 25 (Tier 5)

**Performance Note:**
- Scanning 500+ tickers takes time (~2-5 minutes depending on API speed)
- The S&P 500 sector list is cached for 24 hours (so that part is fast)
- Individual ticker failures don't stop the scan

---

## 🔧 Advanced Features

### Logging
Logs are stored in `logs/` directory with daily rotation:
- **File logs**: Detailed debug information with function names and line numbers
- **Console logs**: Clean, readable output
- **Configurable**: Set `LOG_LEVEL` environment variable (DEBUG, INFO, WARNING, ERROR, CRITICAL)

View logs:
```bash
tail -f logs/jfo_engine_$(date +%Y%m%d).log
```

### Caching
- S&P 500 sector list is cached for 24 hours to reduce Wikipedia API calls
- Cache is stored in `cache/` directory
- Reduces API calls by 99% (from ~5s to <0.1s after first run)
- Automatic cache invalidation based on TTL

### Error Handling
- Automatic retry logic for network requests (3 attempts with exponential backoff)
- Graceful degradation if individual tickers fail
- Comprehensive error logging with stack traces
- Individual ticker failures don't stop the entire scan

### Performance
- Batch downloading with threading
- Multi-index DataFrame handling for efficient data access
- Minimum data validation (50+ rows required)
- Progress tracking during processing

### Security
- Environment variable support (no hardcoded secrets)
- Input validation (ticker symbol validation)
- Secure file handling
- Error sanitization (sensitive data not logged)

---

## 📈 Scoring System

Stocks are scored 0-100 based on:

| Component | Points | Description |
|-----------|--------|-------------|
| **Stage Analysis** | 30 | Perfect Stage 2 alignment (Price > SMA50 > SMA200) |
| **Multi-Timeframe RSI** | 20 | Weekly and Monthly RSI > 50 |
| **Institutional Volume** | 20 | Relative Volume >= 2.0x average |
| **Relative Strength** | 30 | Mansfield RS > 0 (outperforming S&P 500) |

**Tier Ratings:**
- **Tier 1 (85-100)**: Exceptional momentum leaders
- **Tier 2 (70-84)**: Strong performers
- **Tier 3 (50-69)**: Moderate strength
- **Tier 4 (30-49)**: Weak performance
- **Tier 5 (0-29)**: Significant laggards

---

## 🔔 Alert Types

The system detects and alerts on:
- 🚀 **Stage 2 Entry/Exit**: Perfect trend alignment changes
- ⚡ **RS Breakout**: Stock starts outperforming S&P 500
- 📊 **Volume Spikes**: 2x+ normal volume (institutional activity)
- 🚀 **SMA Crossings**: Price crosses above/below key moving averages
- 📈 **RSI Reclaim**: Weekly RSI crosses above 50
- 🔥 **52-Week Highs**: New all-time highs

---

## 🚨 Troubleshooting

### Telegram Not Sending Messages
1. Check credentials in `.env`, `config/config.json`, or GitHub Secrets
2. Verify bot token and chat ID are correct
3. Check logs for error messages
4. Ensure bot has permission to send messages
5. For GitHub Actions: Verify secrets are set correctly in repository settings

### Data Download Failures
1. Check internet connection
2. Verify ticker symbols are valid
3. Check yfinance API status
4. Review logs for specific error messages
5. Check if rate limiting is occurring

### No Alerts Generated
- This is normal if there are no new technical changes
- System only sends alerts when state transitions occur
- Check logs to see what was processed
- Verify that stocks meet the scoring thresholds (≥85 for leaders, ≤25 for laggards)

### GitHub Actions Issues
- **"Telegram Error: Missing credentials"**: Verify secrets are added in GitHub Settings
- **"Workflow runs but no Telegram messages"**: Check bot token and chat ID are correct
- **Secrets not working**: Ensure secret names match exactly: `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`

---

## 🔐 Security Best Practices

1. **Never commit `.env` file** - It's in `.gitignore`
2. **Use environment variables** for sensitive data
3. **Use GitHub Secrets** for automated workflows
4. **Rotate API tokens** periodically
5. **Review logs** for any suspicious activity
6. **Secrets are encrypted** - GitHub encrypts them at rest
7. **Secrets are masked in logs** - GitHub automatically hides them

---

## 📝 Code Quality Features

- ✅ **Type hints** added to key functions
- ✅ **Comprehensive error handling** with retry logic
- ✅ **Detailed logging** with file and console outputs
- ✅ **Well-documented functions** with docstrings
- ✅ **Modular architecture** for easy maintenance
- ✅ **Input validation** for ticker symbols
- ✅ **Backward compatible** - existing workflows continue to work

---

## 🚀 GitHub Actions Setup

### Workflow Configuration

Your workflow (`.github/workflows/main.yml`) is already configured correctly:

```yaml
env:
  TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
  TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
  MANUAL_TICKERS: ${{ github.event.inputs.manual_tickers }}
  LOG_LEVEL: INFO
```

### Schedule
- Runs every **30 minutes** during market hours
- Schedule: `*/30 13-21 * * 1-5` (13:00-21:00 UTC, Monday-Friday)
- Can also be triggered manually via workflow dispatch

### Testing GitHub Actions
1. Go to **Actions** tab in your repository
2. Click on your workflow
3. Click **"Run workflow"** → **"Run Stocks"**
4. Check the logs to verify it's working
5. Verify Telegram messages arrive

---

## 📞 Support

For issues or questions:
1. Check logs in `logs/` directory
2. Review error messages in console output
3. Verify configuration is correct
4. Check GitHub Actions logs if running in Actions
5. Review troubleshooting section above

---

## 📝 License

Private project for Jain Family Office.

---

## 🤝 Contributing

This is a private project. For improvements, please document changes and test thoroughly.

---

**Built with ❤️ for institutional-grade market intelligence**
