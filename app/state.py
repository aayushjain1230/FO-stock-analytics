"""Streamlit session-state helpers."""

from __future__ import annotations

from app.services.watchlist import load_default_watchlist


def ensure_session_state(st) -> None:
    """Initialize Streamlit session state without writing shared files."""
    if "watchlist" not in st.session_state:
        st.session_state.watchlist = load_default_watchlist()
    if "analyses" not in st.session_state:
        st.session_state.analyses = []
    if "v2_intelligence" not in st.session_state:
        st.session_state.v2_intelligence = {}
    if "market" not in st.session_state:
        st.session_state.market = None
    if "last_error" not in st.session_state:
        st.session_state.last_error = None
    if "show_technical_details" not in st.session_state:
        st.session_state.show_technical_details = False
    if "sec_sync_status" not in st.session_state:
        st.session_state.sec_sync_status = {"state": "Never synchronized", "message": "SEC filing sync has not been run in this browser session.", "last_success": None}
