# Watchlist Intelligence - Version 3

Watchlist Intelligence is a small, reliable stock-research app for the stocks you care about.

It answers:

- What changed?
- Why does it matter?
- What is the main risk?
- What should I watch next?
- How confident is the evidence?

This is a research tool. It is not a brokerage, portfolio tracker, trade executor, personal-finance app, or account-management system.

Version 3 adds economically constrained relative-value research, volatility forecasting, regime-aware simulation support, single-stock factor proxy analysis, descriptive risk-adjusted metrics, model comparison contracts, and private methodology reporting. It still does not identify anonymous buyers, analyze dark-pool activity, trade options flow, run live pair trades, optimize portfolios, connect to brokerage accounts, or use AI chat.

## Architecture

The user-facing product stays simple:

```text
streamlit_app.py
main.py
app/
  analysis/
  models/
  services/
  ui/
tests/
config/default_watchlist.json
docs/methodology_v2.md
docs/methodology_v3.md
```

Legacy advanced quant modules remain in the repository for later phases, but they are not imported by the V2 CLI at startup and do not control the Streamlit interface.

## Local setup

Use Python 3.12.

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Create a local `.env` if you want Telegram or SEC synchronization:

```bash
copy .env.example .env
```

Environment variables:

```env
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
SEC_USER_AGENT=FO Stock Analytics your-email@example.com
LOG_LEVEL=INFO
```

Never commit real credentials. `SEC_USER_AGENT` must include an application name and contact email before SEC synchronization is enabled. If it is missing, SEC sync is disabled gracefully and the rest of the app continues working.

## Run the Streamlit app

```bash
streamlit run streamlit_app.py
```

The app has exactly four destinations:

1. Home
2. Stocks
3. Opportunities
4. Settings

There are no user-facing portfolio pages or lab pages in Version 3. Streamlit watchlists are stored in `st.session_state`; they are session-isolated and are not permanent without an account database.

## CLI commands

```bash
python main.py --help
python main.py --list
python main.py --add AAPL MSFT
python main.py --remove TSLA
python main.py --analyze
python main.py --analyze --dry-run-telegram
python main.py --analyze --send-telegram
python main.py --dashboard
```

`--dashboard` writes a simple HTML summary to `plots/watchlist_intelligence.html`. The real interactive app is Streamlit.

Generate the private technical methodology report:

```bash
python main.py --model-report
```

## Telegram

Telegram produces one concise plain-English watchlist brief. It includes at most the most meaningful V2 evidence and does not include portfolio sections, Sharpe ratio, VaR, factor exposure, correlation, cointegration, dark-pool claims, or anonymous-buyer identification.

Use dry-run mode to inspect output without sending:

```bash
python main.py --analyze --dry-run-telegram
```

Telegram delivery failures do not crash analysis.

## Data sources and limitations

Version 2 uses `yfinance` for market data/basic fundamentals and SEC EDGAR for configured filing synchronization.

Market data may be delayed, missing, revised, rate-limited, or unavailable. Missing values are shown as missing; they are not silently treated as negative evidence.

SEC filing interpretation is cautious:

- Form 4 open-market purchases are separated from grants, option exercises, gifts, and tax withholding.
- Schedule 13D can indicate a major holder with possible control-related context, but the filing must be read for exact intent.
- Schedule 13G generally reflects passive or qualifying ownership and is not an activist claim by itself.
- Form 13F is delayed ownership-trend evidence and does not prove what a manager bought or owns today.

Whale Activity means trading behavior is consistent with larger buyers or sellers. It cannot identify anonymous market participants or prove intentions.

Historical-bootstrap and regime-conditioned results are conditional model scenarios. Do not read them as guaranteed real-world probabilities.

Relative-value research starts with economic peers. Correlation alone cannot qualify a pair. Pair outputs are relationship watches, not long/short instructions.

EWMA volatility is the active transparent baseline. GARCH is disabled or marked unavailable unless the optional dependency and validation checks support it.

Sharpe and Sortino are descriptive risk-adjusted consistency metrics, not predictions.

See [docs/methodology_v2.md](docs/methodology_v2.md) for methodology version 2.
See [docs/methodology_v3.md](docs/methodology_v3.md) for methodology version 3.

## Tests

```bash
python -m compileall .
pytest
```

For a Streamlit startup smoke test:

```bash
streamlit run streamlit_app.py --server.headless true
```

Use a timeout/health check in automation so the server process does not hang indefinitely.

## GitHub Actions and jobs

The scheduled workflow runs once after market close on weekdays. It installs dependencies, runs tests, analyzes the watchlist, sends Telegram only when configured, and uploads the analysis artifact.

The five-minute scanner schedule is disabled. Automation does not commit runtime state to `main`.

Separate jobs are intended for daily watchlist analysis, SEC filing synchronization, model refresh, one daily Telegram brief, periodic calibration, and optional manual historical backtests. Runtime state is stored under `state/`, generated reports under `reports/`, and cache data under `cache/`; these are ignored by git.

## Future candidates

- Full factor-model suite
- Broader model comparison
- Advanced quant research labs
- Options-flow analysis
- Portfolio optimization

## Disclaimer

This software is for research and education only. It does not manage real money, execute trades, guarantee returns, or provide financial advice. Always verify data and conclusions independently.
