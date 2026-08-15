"""Version 1 Streamlit entry point."""

from __future__ import annotations

try:
    import streamlit as st
except Exception:  # pragma: no cover - dependency check path
    st = None

from app.navigation import PAGES
from app.services.analysis import analyze_stock
from app.services.configuration import load_config
from app.services.market_data import fetch_market_snapshot, fetch_price_history, fetch_snapshots
from app.services.v2_intelligence import build_v2_intelligence
from app.state import ensure_session_state
from app.ui.styles import STYLE
from app.ui.pages import home, opportunities, settings, stocks


def refresh_data(force_refresh: bool = False) -> None:
    """Refresh market and watchlist analysis into session state."""
    config = load_config()
    tickers = st.session_state.watchlist
    st.session_state.market = fetch_market_snapshot(force_refresh=force_refresh)
    histories = fetch_price_history(tickers, period=config.period, interval=config.interval, force_refresh=force_refresh)
    snapshots = fetch_snapshots(tickers, period=config.period, interval=config.interval, force_refresh=force_refresh)
    st.session_state.analyses = [analyze_stock(snapshots[ticker], histories.get(ticker)) for ticker in tickers]
    st.session_state.v2_intelligence = build_v2_intelligence(
        st.session_state.analyses,
        histories,
        market_condition=st.session_state.market.condition if st.session_state.market else None,
        sync_sec=False,
    )


def main() -> None:
    """Run the Streamlit app."""
    if st is None:
        raise RuntimeError("Streamlit is not installed. Run: python -m pip install -r requirements.txt")
    st.set_page_config(page_title="Watchlist Intelligence", page_icon="📈", layout="wide")
    st.markdown(STYLE, unsafe_allow_html=True)
    ensure_session_state(st)
    st.sidebar.title("Watchlist Intelligence")
    page = st.sidebar.radio("Navigation", PAGES)
    if st.sidebar.button("Refresh data", use_container_width=True):
        with st.spinner("Refreshing market data..."):
            refresh_data(force_refresh=True)
    if not st.session_state.analyses:
        with st.spinner("Loading watchlist data..."):
            refresh_data(force_refresh=False)
    if page == "Home":
        home.render(st)
    elif page == "Stocks":
        stocks.render(st)
    elif page == "Opportunities":
        opportunities.render(st)
    elif page == "Settings":
        settings.render(st)


if __name__ == "__main__":
    main()
