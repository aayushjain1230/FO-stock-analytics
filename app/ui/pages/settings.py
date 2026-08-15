"""Settings page."""

from __future__ import annotations

from app.services.configuration import load_config, telegram_status
from app.services.runtime_store import RuntimeStore
from app.services.sec_cache import SecCache
from app.services.sec_identity import sec_identity_status
from app.services.model_registry import registry_as_dict


def render(st) -> None:
    """Render settings and data-source status."""
    st.title("Settings")
    config = load_config()
    st.subheader("Watchlist Controls")
    st.write(f"Current session watchlist: {', '.join(st.session_state.watchlist)}")
    st.caption("Streamlit watchlists are temporary for this browser session. They do not change scheduled GitHub Actions or Telegram reports.")

    st.subheader("Defaults")
    st.write(f"Benchmark: {config.benchmark}")
    st.write(f"Data period: {config.period}")
    st.write(f"Data interval: {config.interval}")

    st.subheader("Telegram")
    status = telegram_status()
    st.write(f"Connection status: {'Configured' if status['configured'] else 'Not configured'}")
    st.write(f"Daily brief: {'Enabled' if config.daily_brief_enabled else 'Disabled'}")
    st.write(f"Alert time: {config.alert_time}")
    st.write(f"Quiet hours: {config.quiet_hours}")
    st.write(f"Explanation depth: {config.explanation_depth}")

    st.subheader("Data Source Status")
    st.write("Primary source: yfinance. Market data may be delayed, missing, or revised.")
    st.write("SEC source: SEC EDGAR structured submissions and filing documents.")

    st.subheader("SEC Filing Intelligence")
    sec_status = sec_identity_status()
    st.write(f"SEC connection status: {'Configured' if sec_status.configured else 'Disabled'}")
    st.caption(sec_status.message)
    sync_state = st.session_state.sec_sync_status
    st.write(f"SEC sync state: {sync_state['state']}")
    st.caption(sync_state["message"])
    if sync_state.get("last_success"):
        st.caption(f"Last successful sync: {sync_state['last_success']}")
    if st.button("Sync SEC filings now", use_container_width=True):
        st.info("Use the sidebar **Sync SEC filings** button to refresh market data and SEC filing intelligence together.")
    cache_status = SecCache().status()
    st.write(f"Filing cache entries: {cache_status['entries']}")
    st.caption("SEC_USER_AGENT value is never displayed here.")

    st.subheader("Simulation Defaults")
    st.write("Default simulations: 5,000")
    st.write("Default horizon: 30 trading days")
    st.write("Default bootstrap methods: historical, block, and regime-conditioned backend support")
    st.session_state.show_technical_details = st.toggle("Show technical details by default", value=st.session_state.show_technical_details)

    st.subheader("Runtime Store")
    store_status = RuntimeStore().status()
    st.write(f"Runtime database: {store_status['path']}")
    st.json(store_status["counts"])

    st.subheader("Methodology")
    st.write("See docs/methodology_v3.md for relative value, volatility, factor, risk, simulation, and evidence-aggregation methodology.")
    st.write("Private model report: run `python main.py --model-report`.")
    st.json(registry_as_dict())
    st.subheader("Disclaimer")
    st.warning("This is a research tool. It does not connect to brokerage accounts, manage real money, execute trades, or guarantee returns.")
