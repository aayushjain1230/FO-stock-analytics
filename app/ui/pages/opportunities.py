"""Opportunities page."""

from __future__ import annotations

from app.ui.components import analysis_card


def render(st) -> None:
    """Render restrained watchlist opportunities."""
    st.title("Opportunities")
    st.caption("Version 1 opportunities are watchlist observations, not trade recommendations.")
    candidates = [
        item
        for item in st.session_state.analyses
        if item.confidence in {"Medium", "High"} and item.overall_view in {"Positive", "Worth Watching", "Cautious"}
    ]
    candidates = sorted(candidates, key=lambda item: item.attention_score(), reverse=True)[:5]
    if not candidates:
        st.info("No high-confidence watchlist opportunities right now.\n\nSeveral stocks were analyzed, but none had enough independent evidence to qualify.")
    else:
        for item in candidates:
            analysis_card(st, item)
    _render_v2_opportunities(st, bool(candidates))


def _render_v2_opportunities(st, has_v1_candidates: bool) -> None:
    """Render validated V2 opportunities and risks."""
    v2 = getattr(st.session_state, "v2_intelligence", {})
    rows = []
    for item in v2.values():
        for pair in item.relative_values:
            if pair.relationship_status in {"Moderate Divergence", "Unusual Divergence"} and pair.confidence in {"Medium", "High"}:
                rows.append(("Relative-Value Watch", pair.pair_label, pair.relationship_status, f"{pair.divergence_direction} Historical validation: {pair.out_of_sample_status}. This is worth monitoring, not a trade instruction.", pair.confidence))
        if item.whale_activity.level in {"High", "Elevated", "Distribution Risk"}:
            rows.append(("Observed market behavior", item.ticker, f"Whale Activity: {item.whale_activity.level}", item.whale_activity.inference, item.whale_activity.confidence))
        for insight in item.filing_insights:
            rows.append(("Confirmed filing", item.ticker, insight.headline, insight.what_changed, insight.confidence))
        sim = item.simulation
        if sim.scenarios_ending_higher_pct and sim.falling_more_than_10_pct is not None:
            if sim.scenarios_ending_higher_pct >= 58 and sim.falling_more_than_10_pct <= 18:
                rows.append(("Model estimate", item.ticker, "Favorable scenario balance", sim.explanation, sim.confidence))
            elif sim.falling_more_than_10_pct >= 25:
                rows.append(("Model estimate", item.ticker, "Meaningful downside frequency", sim.explanation, sim.confidence))
    if rows:
        st.subheader("Version 2 Opportunities and Risks")
        for category, ticker, title, body, confidence in rows[:6]:
            st.write(f"**{category}: {ticker} - {title}**")
            st.write(body)
            st.caption(f"Confidence: {confidence}")
    elif not has_v1_candidates:
        st.info("No Version 2 opportunities passed validation. That is a valid outcome; the system is allowed to abstain.")
