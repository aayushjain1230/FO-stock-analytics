# 📈 Jain Family Office: Market Intelligence Engine

An automated institutional-grade market scanner and alerting system. This engine monitors your personal watchlist and the S&P 500 sectors, generating technical analysis reports and visual dashboards directly to Telegram.

---

## 🚀 Key Features

* **Dual-Layer Scanning:** Tracks your custom `watchlist.json` + scans the entire S&P 500 for momentum leaders and laggards.
* **Smart Alerts:** Only notifies you via Telegram when data actually changes (MD5 Fingerprinting).
* **Executive Dashboards:** Generates high-resolution multi-panel charts of your entire watchlist vs. the S&P 500 benchmark.
* **Market Gatekeeper:** Automatically respects NYSE market hours and holidays to save on compute minutes.
* **GitHub Actions Ready:** Fully automated via cron schedules—no server required.

---

## 📂 Project Structure

| File / Folder | Purpose |
| :--- | :--- |
| `main.py` | The central brain; handles logic, orchestration, and scheduling. |
| `watchlist.json` | Persistent list of your favorite stock tickers. |
| `plots/` | Stores generated PNG dashboards and technical charts. |
| `state/` | Stores historical data and report hashes for deduplication. |
| `indicators.py` | TA logic (RSI, Moving Averages, RS Line). |
| `telegram_notifier.py` | Handles message bundling and image uploads to Telegram. |

---

## 🛠 Setup & Installation

### 1. Requirements
Ensure you have Python 3.9+ installed, then run:
```bash
pip install -r requirements.txt



