"""Home page."""

from __future__ import annotations

import pandas as pd

from app.services.analysis import daily_summary
from app.ui.components import analysis_card, pct


def render(st) -> None:
    """Render the Home destination."""
    st.title("Watchlist Intelligence")
    market = st.session_state.market
    analyses = st.session_state.analyses

    st.subheader("Market Today")
    if market is None:
        st.info("Market data has not been loaded yet. Use Refresh Data in Settings.")
    else:
        cols = st.columns(4)
        cols[0].metric("S&P 500", pct(market.sp500_change_pct))
        cols[1].metric("Nasdaq", pct(market.nasdaq_change_pct))
        cols[2].metric("Condition", market.condition)
        cols[3].metric("Last updated", market.updated_at.strftime("%H:%M UTC"))
        st.write(market.explanation)

    st.subheader("Attention Needed")
    attention = sorted(analyses, key=lambda item: item.attention_score(), reverse=True)[:3]
    if not attention:
        st.info("No high-confidence watchlist opportunities right now.")
    for item in attention:
        analysis_card(st, item)

    _render_v2_signals(st)

    st.subheader("Watchlist Overview")
    if analyses:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "Ticker": a.ticker,
                        "Price": a.snapshot.price,
                        "Daily move": pct(a.snapshot.daily_change_pct),
                        "Five-day move": pct(a.snapshot.five_day_change_pct),
                        "Trend": a.trend,
                        "Volume": a.volume_status,
                        "Overall view": a.overall_view,
                        "Last updated": a.snapshot.updated_at.strftime("%Y-%m-%d %H:%M"),
                    }
                    for a in analyses
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No watchlist analysis loaded yet.")

    st.subheader("Upcoming Events")
    events = [f"{a.ticker}: earnings {a.snapshot.next_earnings_date}" for a in analyses if a.snapshot.next_earnings_date]
    if events:
        for event in events[:5]:
            st.write(f"• {event}")
    else:
        st.caption("No reliable upcoming events are available from the current data source.")

    st.subheader("Daily Summary")
    st.write(daily_summary(analyses))


def _render_v2_signals(st) -> None:
    """Render only material Version 2 highlights on Home."""
    v2 = getattr(st.session_state, "v2_intelligence", {})
    material_filings = [insight for item in v2.values() for insight in item.filing_insights]
    elevated_whales = [item for item in v2.values() if item.whale_activity.level in {"High", "Elevated", "Distribution Risk"}]
    event_warnings = [item for item in v2.values() if item.simulation.event_warning]
    relative = [pair for item in v2.values() for pair in item.relative_values if pair.relationship_status in {"Moderate Divergence", "Unusual Divergence"} and pair.confidence in {"Medium", "High"}]

    st.subheader("SEC, Whale Activity, and Event Risk")
    if material_filings:
        insight = material_filings[0]
        st.info(f"**{insight.ticker} - {insight.headline}**\n\n{insight.what_changed}\n\nWhy it matters: {insight.why_it_matters}")
    if elevated_whales:
        whale = sorted(elevated_whales, key=lambda item: item.whale_activity.internal_score or 0, reverse=True)[0].whale_activity
        st.warning(f"**{whale.ticker} - Whale Activity: {whale.level}**\n\n{whale.inference}\n\nConfidence: {whale.confidence}")
    if event_warnings:
        sim = event_warnings[0].simulation
        st.warning(f"**{sim.ticker} - Event-aware simulation warning**\n\n{sim.event_warning}")
    if relative:
        pair = relative[0]
        st.info(f"**Relative-Value Watch: {pair.pair_label}**\n\n{pair.divergence_direction}\n\nConfidence: {pair.confidence}")
    if not material_filings and not elevated_whales and not event_warnings and not relative:
        st.caption("No material SEC filing, Whale Activity, relative-value, or event-risk update passed the current filters.")
