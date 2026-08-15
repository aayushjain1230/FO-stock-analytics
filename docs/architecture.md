# Architecture

## Official product surface

The official application is the Streamlit app at `streamlit_app.py`.

Primary navigation is intentionally limited to:

- Home
- Stocks
- Opportunities
- Settings

No portfolio pages or lab pages are part of the official product.

## Watchlist behavior

There are three separate watchlist contexts:

1. Streamlit visitor: session-isolated, temporary browser state.
2. Local CLI user: may persist local ignored `watchlist.json`.
3. GitHub Actions: uses manual workflow input, then `WATCHLIST_TICKERS`, then `config/default_watchlist.json`.

Changing the Streamlit watchlist does not change scheduled GitHub Actions or Telegram reports.

## Legacy audit

Retained but non-authoritative:

- `quant_dashboard.py`: legacy generated dashboard system; not official startup.
- `frontend/`: legacy Flask/template prototype; not imported by `streamlit_app.py`.
- `frontend/templates/*`: old lab/portfolio templates retained as archived reference, not product pages.
- `frontend/static/*`: legacy assets retained as archived reference.
- `portfolio_engine.py`: future backend research code, not user-facing portfolio tracking.
- `telegram_notifier.py`: legacy notifier kept for old tests; official daily brief lives in `app/services/telegram.py`.
- `manage_watchlist.py`: legacy helper; official watchlist resolution lives in `app/services/watchlist.py`.

The authoritative app path is `streamlit_app.py` plus the `app/` package.
