"""Reusable Streamlit components."""

from __future__ import annotations

from app.models.stock import StockAnalysis


def pct(value: float | None) -> str:
    """Format a decimal return as percent."""
    if value is None:
        return "N/A"
    return f"{value * 100:+.2f}%"


def analysis_card(st, analysis: StockAnalysis) -> None:
    """Render one attention/stock card."""
    st.markdown(
        f"""
<div class="v1-card">
  <h3>{analysis.ticker} • {analysis.trend}</h3>
  <p><b>What changed:</b><br>{analysis.what_changed}</p>
  <p><b>Why it matters:</b><br>{analysis.why_it_matters}</p>
  <p><b>Main risk:</b><br>{analysis.main_risk}</p>
  <p><b>Watch next:</b><br>{analysis.what_to_watch}</p>
  <p class="muted small">Confidence: {analysis.confidence} • Updated: {analysis.snapshot.updated_at.strftime('%Y-%m-%d %H:%M UTC')}</p>
</div>
""",
        unsafe_allow_html=True,
    )
