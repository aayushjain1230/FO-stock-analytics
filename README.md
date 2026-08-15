# Watchlist Intelligence

Watchlist Intelligence is a Streamlit stock-research dashboard for a focused watchlist.

It answers:

- What changed?
- Why does it matter?
- What is the main risk?
- What should I watch next?

This is a research tool. It is not a brokerage, portfolio tracker, account manager, or trade executor.

## Official dashboard

The official interactive dashboard is:

```bash
streamlit run streamlit_app.py
```

The app has exactly four primary destinations:

1. Home
2. Stocks
3. Opportunities
4. Settings

No portfolio pages or research lab pages are part of the official Streamlit product.

## Optional static report

Generate a static report artifact:

```bash
python main.py --export-report
```

Output:

```text
reports/watchlist_report.html
```

The static report is not the dashboard. Open the Streamlit application for interactive analysis.

`--dashboard` is retained only as a deprecated alias and prints guidance to use `--export-report`.

## Watchlist precedence

GitHub Actions and CLI analysis resolve tickers in this order:

1. Manual workflow or CLI `--tickers` input
2. `WATCHLIST_TICKERS` environment variable or GitHub repository variable
3. `config/default_watchlist.json`

Scheduled workflows cannot ask for input. Configure scheduled tickers at:

```text
Repository Settings -> Secrets and variables -> Actions -> Variables -> WATCHLIST_TICKERS
```

Example:

```text
AAPL,MSFT,NVDA,AMD,GOOGL
```

This is not sensitive and belongs in a repository variable, not a secret.

Streamlit watchlist edits are temporary browser-session changes. They do not update scheduled GitHub Actions or Telegram reports.

## CLI

```bash
python main.py --help
python main.py --list
python main.py --add AAPL MSFT
python main.py --remove TSLA
python main.py --analyze
python main.py --analyze --tickers AAPL MSFT NVDA
python main.py --analyze --tickers AAPL,MSFT,NVDA
WATCHLIST_TICKERS=AAPL,MSFT,NVDA python main.py --analyze
python main.py --analyze --send-telegram
python main.py --analyze --sync-sec
python main.py --export-report
python main.py --model-report
```

The final resolved watchlist is printed before analysis and included in `state/v2_latest_analysis.json`.

## GitHub Actions

Manual workflow runs ask for:

- Watchlist
- Whether to send Telegram
- Whether to sync SEC filings
- Whether to export a static report artifact

Scheduled runs use `WATCHLIST_TICKERS` when configured, otherwise `config/default_watchlist.json`.

Artifacts are downloadable at:

```text
GitHub repository -> Actions -> Select workflow run -> Artifacts
```

Artifacts are not permanent hosting.

## SEC synchronization

SEC synchronization requires:

```env
SEC_USER_AGENT=FO Stock Analytics your-email@example.com
```

If missing, SEC sync is disabled safely and ordinary analysis still works. The Streamlit UI never displays the full User-Agent value.

SEC requests are run only through explicit Streamlit action, CLI `--sync-sec`, or bounded background jobs. Reruns do not automatically poll the SEC.

## Deployment

See [docs/deployment.md](docs/deployment.md).

For Streamlit Community Cloud:

- Repository: `aayushjain1230/FO-stock-analytics`
- Branch: `main`
- Main file path: `streamlit_app.py`

Optional Streamlit secrets:

```toml
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = ""
SEC_USER_AGENT = ""
```

## Architecture and legacy files

See [docs/architecture.md](docs/architecture.md).

The legacy `frontend/` templates and `quant_dashboard.py` are not the official dashboard. They are retained as non-authoritative legacy/reference code and are not imported by `streamlit_app.py`.

## Tests

```bash
python -m compileall .
python -m pytest
```

## Disclaimer

This software is for research and education only. It does not manage real money, execute trades, guarantee returns, or provide financial advice. Always verify data and conclusions independently.
