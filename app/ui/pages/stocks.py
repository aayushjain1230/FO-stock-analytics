"""Stocks page."""

from __future__ import annotations

from app.services.watchlist import add_ticker, remove_ticker
from app.ui.components import analysis_card, pct


def render(st) -> None:
    """Render watchlist management and stock deep dive."""
    st.title("Stocks")
    st.caption("Session watchlists are isolated per visitor and are not permanent without an account database.")
    left, right = st.columns([2, 1])
    new_ticker = left.text_input("Add ticker", placeholder="AAPL")
    if right.button("Add", use_container_width=True):
        updated, error = add_ticker(st.session_state.watchlist, new_ticker)
        if error:
            st.error(error)
        else:
            st.session_state.watchlist = updated
            st.success(f"Added {new_ticker.upper()}")

    sort_by = st.selectbox(
        "Sort by",
        ["Alphabetical", "Largest daily move", "Largest five-day move", "Improving", "Weakening", "Unusual volume", "Upcoming earnings"],
    )
    analyses = _sort(st.session_state.analyses, sort_by)
    selected = st.selectbox("Selected stock", st.session_state.watchlist)
    cols = st.columns([1, 1, 1])
    if cols[0].button("Remove selected"):
        st.session_state.watchlist = remove_ticker(st.session_state.watchlist, selected)
        st.rerun()

    st.subheader("Watchlist")
    for ticker in st.session_state.watchlist:
        st.write(f"• {ticker}")

    st.subheader("Stock Deep Dive")
    analysis = next((item for item in analyses if item.ticker == selected), None)
    if analysis is None:
        st.info("Refresh data to analyze this stock.")
        return
    analysis_card(st, analysis)
    st.markdown("### Three-point summary")
    st.write(f"**Business Trend:** fundamentals are {'partially available' if analysis.snapshot.market_cap else 'not fully available'} from the current source.")
    st.write(f"**Market Behavior:** {analysis.what_changed}")
    st.write(f"**Risk Outlook:** {analysis.main_risk}")
    st.markdown("### Price and Volume")
    st.write(f"Price: {analysis.snapshot.price} • Daily: {pct(analysis.snapshot.daily_change_pct)} • Volume: {analysis.volume_status}")
    st.markdown("### Basic Fundamentals")
    st.json({
        "market_cap": analysis.snapshot.market_cap,
        "revenue_growth": analysis.snapshot.revenue_growth,
        "earnings_growth": analysis.snapshot.earnings_growth,
        "profit_margin": analysis.snapshot.profit_margin,
        "debt_to_equity": analysis.snapshot.debt_to_equity,
        "free_cash_flow": analysis.snapshot.free_cash_flow,
        "forward_pe": analysis.snapshot.forward_pe,
        "next_earnings_date": analysis.snapshot.next_earnings_date,
    })
    st.markdown("### Evidence")
    st.write("Positive evidence")
    for evidence in analysis.positive_evidence:
        st.write(f"• {evidence.interpretation}")
    st.write("Warning signs")
    for evidence in analysis.negative_evidence:
        st.write(f"• {evidence.interpretation}")
    if analysis.unknowns:
        st.write("Unknowns")
        for unknown in analysis.unknowns[:6]:
            st.write(f"• {unknown}")

    _render_v2_stock_sections(st, selected)


def _render_v2_stock_sections(st, selected: str) -> None:
    """Render Version 2 details inside the selected stock page."""
    v2 = getattr(st.session_state, "v2_intelligence", {}).get(selected)
    if not v2:
        return

    st.markdown("### Whale Activity")
    whale = v2.whale_activity
    st.write(f"**Level:** {whale.level}")
    st.write(f"**Confidence:** {whale.confidence}")
    st.write(f"**What changed:** {whale.inference}")
    if whale.evidence:
        st.write("Supporting evidence")
        for evidence in whale.evidence[:4]:
            st.write(f"- {evidence.interpretation}")
    if whale.contradicting_evidence:
        st.write("Contradicting evidence")
        for evidence in whale.contradicting_evidence[:3]:
            st.write(f"- {evidence.interpretation}")
    if whale.confirmed_filings:
        st.write("Filing confirmation")
        for filing in whale.confirmed_filings[:2]:
            st.write(f"- {filing.form_type}: {filing.transaction_type or 'filing detected'}")
    if whale.confounders:
        st.write("Confounders")
        for item in whale.confounders:
            st.write(f"- {item}")
    st.caption("Whale Activity estimates whether trading behavior is consistent with larger buyers or sellers. It cannot identify anonymous market participants or prove intentions.")

    st.markdown("### Model Outlook")
    sim = v2.simulation
    st.write(f"**Horizon:** {sim.horizon_days} trading days")
    st.write(f"**Outlook:** {sim.model_outlook}")
    st.write(f"**Risk level:** {sim.risk_level}")
    if sim.scenarios_ending_higher_pct is not None:
        st.write(f"**Scenarios finishing higher:** {sim.scenarios_ending_higher_pct:.0f}%")
    if sim.percentile_range:
        st.write(f"**Most common model range:** ${sim.percentile_range.get('10', 0):.2f} to ${sim.percentile_range.get('90', 0):.2f}")
    st.write(f"**Confidence:** {sim.confidence}")
    st.write(sim.explanation)
    if sim.event_warning:
        st.warning(sim.event_warning)

    if v2.filing_insights:
        st.markdown("### SEC Filing Intelligence")
        for insight in v2.filing_insights[:3]:
            st.write(f"**{insight.headline}**")
            st.write(insight.what_changed)
            st.caption(f"{insight.source_label}: {insight.source_url}")

    st.markdown("### Relative Value")
    qualified_pairs = [pair for pair in v2.relative_values if pair.relationship_status in {"Moderate Divergence", "Unusual Divergence", "Normal Relationship"} and pair.confidence != "Low"]
    if qualified_pairs:
        pair = qualified_pairs[0]
        st.write(f"**Relative Value: {pair.relationship_status}**")
        st.write(pair.divergence_direction)
        st.caption("This does not prove fundamental undervaluation and is not a pair-trade instruction.")
        st.write(f"**Main risk:** {pair.limitations[0] if pair.limitations else 'The relationship may be changing for valid company-specific reasons.'}")
        st.write(f"**Confidence:** {pair.confidence}")
    else:
        st.caption("No reliable peer relationship qualified for this stock.")

    st.markdown("### Expected Movement")
    vol = v2.volatility
    st.write(f"**Expected Price Movement:** {vol.expected_movement}")
    st.write(vol.why)
    st.write(f"**Main limitation:** {vol.main_limitation}")
    st.caption(f"Confidence: {vol.confidence}")

    st.markdown("### Performance Drivers and Risk Consistency")
    st.write(f"**{v2.factor_model.conclusion}**")
    st.caption(v2.factor_model.residual_interpretation)
    st.write(f"**{v2.risk_metrics.conclusion}**")

    with st.expander("Technical Details"):
        st.json(
            {
                "whale_component_scores": whale.component_scores,
                "simulation": {
                    "method": sim.method,
                    "simulation_count": sim.simulations,
                    "interval_percentiles": sim.percentile_range,
                    "drawdown_percentiles": sim.max_drawdown_percentiles,
                    "calibration_sample_size": sim.calibration_sample_size,
                    "model_version": sim.model_version,
                },
                "sec_accession_numbers": [record.accession_number for record in v2.filings],
                "source_links": [record.source_url for record in v2.filings],
                "relative_value": [pair.to_dict() for pair in v2.relative_values],
                "volatility": vol.to_dict(),
                "factor_model": v2.factor_model.to_dict(),
                "risk_metrics": v2.risk_metrics.to_dict(),
            }
        )


def _sort(analyses, sort_by):
    if sort_by == "Largest daily move":
        return sorted(analyses, key=lambda a: abs(a.snapshot.daily_change_pct or 0), reverse=True)
    if sort_by == "Largest five-day move":
        return sorted(analyses, key=lambda a: abs(a.snapshot.five_day_change_pct or 0), reverse=True)
    if sort_by == "Improving":
        return sorted(analyses, key=lambda a: a.trend == "Improving", reverse=True)
    if sort_by == "Weakening":
        return sorted(analyses, key=lambda a: a.trend == "Weakening", reverse=True)
    if sort_by == "Unusual volume":
        return sorted(analyses, key=lambda a: a.volume_status == "Unusual volume", reverse=True)
    if sort_by == "Upcoming earnings":
        return sorted(analyses, key=lambda a: bool(a.snapshot.next_earnings_date), reverse=True)
    return sorted(analyses, key=lambda a: a.ticker)
