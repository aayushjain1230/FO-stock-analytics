"""
JFO Quant Research Dashboard
Generates a fully self-contained HTML dashboard from state JSON files and plot PNGs.
All images are base64-encoded so the file works on GitHub Pages with no broken links.
"""

import base64
import html
import json
import os
from datetime import datetime

STATE_DIR = "state"
PLOTS_DIR = "plots"
DASHBOARD_FILE = os.path.join(PLOTS_DIR, "quant_research_dashboard.html")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_json(path, fallback=None):
    if fallback is None:
        fallback = {}
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return fallback


def _b64_image(path):
    """Return an <img> tag with the image embedded as base64, or empty string."""
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "rb") as f:
            data = base64.b64encode(f.read()).decode("utf-8")
        return f'<img src="data:image/png;base64,{data}" style="width:100%;border-radius:6px;margin-top:10px;" loading="lazy">'
    except Exception:
        return ""


def _fmt(value, precision=2, suffix=""):
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.{precision}f}{suffix}"
    except Exception:
        return html.escape(str(value))


def _pct(value, multiply=False):
    if value is None:
        return "N/A"
    try:
        v = float(value) * (100 if multiply else 1)
        return f"{v:.2f}%"
    except Exception:
        return "N/A"


def _score_color(score):
    try:
        s = float(score)
        if s >= 70:
            return "#2ea043"
        if s >= 45:
            return "#d29922"
        return "#da3633"
    except Exception:
        return "#8b949e"


def _score_badge(score, label=None):
    color = _score_color(score)
    text = html.escape(str(label or score))
    return (
        f'<span style="background:{color};color:#fff;padding:2px 8px;'
        f'border-radius:10px;font-size:12px;font-weight:700;">{text}</span>'
    )


def _bar(pct, color="#238636", height=14):
    """Simple CSS progress bar."""
    w = min(max(float(pct or 0), 0), 100)
    return (
        f'<div style="background:#21262d;border-radius:4px;height:{height}px;width:100%;">'
        f'<div style="background:{color};width:{w:.1f}%;height:100%;border-radius:4px;"></div></div>'
    )


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _section_portfolio_overview(portfolio):
    if not portfolio:
        return "<section id='portfolio-overview'><h2>Portfolio Overview</h2><p>Run <code>python main.py --portfolio-report</code> to generate data.</p></section>"

    health = portfolio.get("portfolio_health", {})
    variance = portfolio.get("variance", {})
    sharpe_data = portfolio.get("sharpe", {})
    corr = portfolio.get("correlation", {})
    div = portfolio.get("diversification", {})
    why = portfolio.get("why_now", {})

    health_score = health.get("score", 0)
    health_class = health.get("classification", "N/A")
    health_color = _score_color(health_score)

    alert_html = ""
    if why.get("send_alert"):
        alert_html = f"""
        <div style="background:#2d1b00;border:1px solid #d29922;border-radius:6px;padding:14px;margin:16px 0;">
          <div style="color:#d29922;font-weight:700;margin-bottom:6px;">⚠ Portfolio Alert — Why Now</div>
          <div style="color:#e6edf3;">{html.escape(str(why.get('reason','')))} — {html.escape(str(why.get('evidence','')))} </div>
          <div style="color:#8b949e;margin-top:6px;font-size:13px;">What to watch: {html.escape(str(why.get('what_to_watch','')))} </div>
        </div>"""

    metrics = [
        ("Health Score", f"{_fmt(health_score, 0)}/100", health_color),
        ("Classification", html.escape(str(health_class)), "#8b949e"),
        ("Annual Volatility", _pct(variance.get("annual_volatility"), multiply=True), "#da3633" if (variance.get("annual_volatility") or 0) > 0.25 else "#2ea043"),
        ("Sharpe Ratio", _fmt(sharpe_data.get("sharpe_ratio")), "#2ea043" if (sharpe_data.get("sharpe_ratio") or 0) > 1 else "#d29922"),
        ("Sharpe Class", html.escape(str(sharpe_data.get("classification", "N/A"))), "#8b949e"),
        ("Max Drawdown", _pct(portfolio.get("maximum_drawdown"), multiply=True), "#da3633"),
        ("Avg Correlation", _fmt(corr.get("average_correlation")), "#d29922"),
        ("Diversification", f"{_fmt(div.get('score'), 0)}/100", _score_color(div.get("score", 0))),
        ("Risk Class", html.escape(str(variance.get("risk_classification", "N/A"))), "#8b949e"),
        ("Div Classification", html.escape(str(div.get("classification", "N/A"))), "#8b949e"),
    ]

    cards_html = ""
    for label, value, color in metrics:
        cards_html += f"""
        <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;">
          <div style="color:#8b949e;font-size:12px;margin-bottom:6px;">{label}</div>
          <div style="color:{color};font-size:22px;font-weight:700;">{value}</div>
        </div>"""

    return f"""
    <section id="portfolio-overview">
      <h2>Portfolio Overview</h2>
      {alert_html}
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-top:16px;">
        {cards_html}
      </div>
    </section>"""


def _section_executive_chart():
    img = _b64_image(os.path.join(PLOTS_DIR, "executive_dashboard.png"))
    if not img:
        return ""
    return f"""
    <section id="executive-chart">
      <h2>Executive Dashboard</h2>
      {img}
    </section>"""


def _section_portfolio_risk(portfolio):
    if not portfolio:
        return ""

    # Sector exposure bars
    sector_exposure = portfolio.get("sector_exposure", {})
    sector_html = ""
    if sector_exposure:
        sorted_sectors = sorted(sector_exposure.items(), key=lambda x: x[1], reverse=True)
        for sector, weight in sorted_sectors:
            color = "#1f6feb" if "Tech" in sector else "#238636" if "Health" in sector else "#d29922" if "Finance" in sector else "#58a6ff"
            sector_html += f"""
            <div style="margin-bottom:10px;">
              <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                <span style="color:#e6edf3;font-size:13px;">{html.escape(str(sector))}</span>
                <span style="color:#8b949e;font-size:13px;">{_fmt(weight, 1)}%</span>
              </div>
              {_bar(weight, color)}
            </div>"""

    # Risk contributions
    risk_contrib = portfolio.get("risk_contributions", {})
    risk_html = ""
    if risk_contrib:
        top_risks = sorted(risk_contrib.items(), key=lambda x: x[1], reverse=True)[:6]
        for ticker, pct in top_risks:
            color = "#da3633" if pct > 30 else "#d29922" if pct > 15 else "#2ea043"
            risk_html += f"""
            <div style="margin-bottom:10px;">
              <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                <span style="color:#e6edf3;font-size:13px;font-weight:700;">{html.escape(str(ticker))}</span>
                <span style="color:{color};font-size:13px;">{_fmt(pct, 1)}%</span>
              </div>
              {_bar(pct, color)}
            </div>"""

    # Monte Carlo
    mc = portfolio.get("monte_carlo", {})
    mc_html = ""
    if mc and not mc.get("message"):
        horizon = mc.get("horizon_days", 126)
        horizon_label = f"{max(1, round(horizon / 21))}M" if horizon else "6M"
        expected_return = mc.get("expected_return")
        probability_of_loss = mc.get("probability_of_loss")
        probability_of_large_drawdown = mc.get("probability_of_large_drawdown")
        mc_html = f"""
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-top:12px;">
          <div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px;">
            <div style="color:#8b949e;font-size:12px;">Expected {horizon_label} Return</div>
            <div style="color:#2ea043;font-size:20px;font-weight:700;">{_pct(expected_return, multiply=True)}</div>
          </div>
          <div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px;">
            <div style="color:#8b949e;font-size:12px;">Probability of Loss</div>
            <div style="color:#da3633;font-size:20px;font-weight:700;">{_pct(probability_of_loss, multiply=True)}</div>
          </div>
          <div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px;">
            <div style="color:#8b949e;font-size:12px;">15% Drawdown Risk</div>
            <div style="color:#d29922;font-size:20px;font-weight:700;">{_pct(probability_of_large_drawdown, multiply=True)}</div>
          </div>
        </div>"""

    # Optimization
    opt = portfolio.get("optimization", {})
    opt_html = ""
    if opt and not opt.get("message"):
        curr_vol = opt.get("current_volatility", 0)
        opt_vol = opt.get("optimized_volatility", 0)
        reduction = opt.get("volatility_reduction_pct", 0)
        opt_html = f"""
        <div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:14px;margin-top:12px;">
          <div style="color:#8b949e;font-size:12px;margin-bottom:10px;">MARKOWITZ OPTIMIZATION</div>
          <div style="display:flex;gap:24px;flex-wrap:wrap;">
            <div><div style="color:#8b949e;font-size:11px;">Current Volatility</div><div style="color:#da3633;font-size:18px;font-weight:700;">{_pct(curr_vol, multiply=True)}</div></div>
            <div><div style="color:#8b949e;font-size:11px;">Optimized Volatility</div><div style="color:#2ea043;font-size:18px;font-weight:700;">{_pct(opt_vol, multiply=True)}</div></div>
            <div><div style="color:#8b949e;font-size:11px;">Potential Reduction</div><div style="color:#58a6ff;font-size:18px;font-weight:700;">{_fmt(reduction, 1)}%</div></div>
          </div>
        </div>"""

    # Factor exposure
    factor = portfolio.get("factor_exposure", {})
    factor_html = ""
    if factor and not factor.get("message"):
        drivers = factor.get("main_risk_drivers", {})
        sorted_factors = sorted(drivers.items(), key=lambda x: x[1], reverse=True)
        factor_rows = ""
        for fname, fpct in sorted_factors:
            factor_rows += f"""
            <div style="margin-bottom:8px;">
              <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
                <span style="color:#e6edf3;font-size:12px;">{html.escape(str(fname))}</span>
                <span style="color:#8b949e;font-size:12px;">{_fmt(fpct, 1)}%</span>
              </div>
              {_bar(fpct, "#58a6ff", 10)}
            </div>"""
        factor_html = f"""
        <div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:14px;margin-top:12px;">
          <div style="color:#8b949e;font-size:12px;margin-bottom:10px;">FACTOR EXPOSURE</div>
          {factor_rows}
          <div style="color:#6e7681;font-size:11px;margin-top:6px;">{html.escape(str(factor.get('interpretation','')))} </div>
        </div>"""

    # Correlation pairs
    corr_data = portfolio.get("correlation", {})
    corr_html = ""
    redundant = corr_data.get("redundant_holdings", [])
    if redundant:
        rows = ""
        for item in redundant[:5]:
            pair = item.get("pair", [])
            c = item.get("correlation", 0)
            color = "#da3633" if c > 0.9 else "#d29922"
            rows += f"<tr><td>{'↔'.join(pair)}</td><td style='color:{color};font-weight:700;'>{_fmt(c)}</td><td style='color:#da3633;'>Redundant</td></tr>"
        corr_html = f"""
        <div style="margin-top:12px;">
          <div style="color:#8b949e;font-size:12px;margin-bottom:8px;">HIGH CORRELATION PAIRS (REDUNDANCY RISK)</div>
          <table style="font-size:13px;">
            <thead><tr><th>Pair</th><th>Correlation</th><th>Status</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>"""

    return f"""
    <section id="portfolio-risk">
      <h2>Portfolio Risk Intelligence</h2>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:16px;">
        <div>
          <div style="color:#8b949e;font-size:12px;margin-bottom:10px;letter-spacing:1px;">SECTOR EXPOSURE</div>
          {sector_html or '<p style="color:#6e7681;">No sector data available.</p>'}
        </div>
        <div>
          <div style="color:#8b949e;font-size:12px;margin-bottom:10px;letter-spacing:1px;">RISK CONTRIBUTIONS</div>
          {risk_html or '<p style="color:#6e7681;">No risk contribution data available.</p>'}
        </div>
      </div>
      <div style="color:#8b949e;font-size:12px;margin:20px 0 10px;letter-spacing:1px;">MONTE CARLO SIMULATION</div>
      {mc_html or '<p style="color:#6e7681;">No Monte Carlo data. Run portfolio report first.</p>'}
      {opt_html}
      {factor_html}
      {corr_html}
    </section>"""


def _section_watchlist(rows):
    if not rows:
        return "<section id='watchlist'><h2>Watchlist</h2><p>No watchlist data. Run the analysis engine first.</p></section>"

    sorted_rows = sorted(rows, key=lambda r: float(r.get("quant_score") or r.get("score") or 0), reverse=True)

    table_rows = ""
    for row in sorted_rows:
        ticker = html.escape(str(row.get("ticker", "")))
        score = row.get("quant_score") or row.get("score") or 0
        label = html.escape(str(row.get("quant_label") or row.get("rating", "N/A")))
        close = _fmt(row.get("close"))
        sharpe = _fmt(row.get("sharpe_ratio"))
        vol = _pct(row.get("annualized_volatility"), multiply=True)
        dd = _pct(row.get("max_drawdown"), multiply=True)
        regime = html.escape(str(row.get("volatility_regime", "N/A")))

        score_color = _score_color(score)
        regime_color = "#da3633" if "High" in str(regime) else "#d29922" if "Normal" in str(regime) else "#2ea043"

        table_rows += f"""<tr>
          <td style="font-weight:700;color:#e6edf3;">{ticker}</td>
          <td><span style="color:{score_color};font-weight:700;font-size:15px;">{score}</span></td>
          <td>{_score_badge(score, label)}</td>
          <td style="color:#e6edf3;">${close}</td>
          <td style="color:{'#2ea043' if str(sharpe) != 'N/A' and float(sharpe or 0) > 1 else '#d29922'};">{sharpe}</td>
          <td>{vol}</td>
          <td style="color:#da3633;">{dd}</td>
          <td><span style="color:{regime_color};font-size:12px;">{regime}</span></td>
        </tr>"""

    return f"""
    <section id="watchlist">
      <h2>Watchlist Rankings</h2>
      <div style="overflow-x:auto;margin-top:16px;">
        <table>
          <thead><tr>
            <th>Ticker</th><th>Score</th><th>Rating</th><th>Close</th>
            <th>Sharpe</th><th>Ann.Vol</th><th>Max DD</th><th>Vol Regime</th>
          </tr></thead>
          <tbody>{table_rows}</tbody>
        </table>
      </div>
    </section>"""


def _section_stock_charts(rows):
    if not rows:
        return ""

    cards = ""
    for row in rows:
        ticker = str(row.get("ticker", ""))
        score = row.get("quant_score") or row.get("score") or 0
        label = row.get("quant_label") or row.get("rating", "N/A")
        sharpe = _fmt(row.get("sharpe_ratio"))
        vol = _pct(row.get("annualized_volatility"), multiply=True)
        dd = _pct(row.get("max_drawdown"), multiply=True)
        regime = row.get("volatility_regime", "N/A")
        note = html.escape(str(row.get("research_note", "")))

        img = _b64_image(os.path.join(PLOTS_DIR, f"{ticker}_analysis.png"))
        score_color = _score_color(score)

        cards += f"""
        <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin-bottom:20px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
            <div style="color:#e6edf3;font-size:18px;font-weight:700;">{html.escape(ticker)}</div>
            <div style="display:flex;gap:10px;align-items:center;">
              <span style="color:{score_color};font-size:22px;font-weight:700;">{score}</span>
              {_score_badge(score, label)}
            </div>
          </div>
          <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px;">
            <div style="background:#0d1117;border-radius:4px;padding:8px;">
              <div style="color:#6e7681;font-size:11px;">Sharpe</div>
              <div style="color:#e6edf3;font-weight:700;">{sharpe}</div>
            </div>
            <div style="background:#0d1117;border-radius:4px;padding:8px;">
              <div style="color:#6e7681;font-size:11px;">Ann. Vol</div>
              <div style="color:#e6edf3;font-weight:700;">{vol}</div>
            </div>
            <div style="background:#0d1117;border-radius:4px;padding:8px;">
              <div style="color:#6e7681;font-size:11px;">Max DD</div>
              <div style="color:#da3633;font-weight:700;">{dd}</div>
            </div>
            <div style="background:#0d1117;border-radius:4px;padding:8px;">
              <div style="color:#6e7681;font-size:11px;">Regime</div>
              <div style="color:#d29922;font-size:12px;">{html.escape(str(regime))}</div>
            </div>
          </div>
          {img if img else '<div style="color:#6e7681;padding:20px;text-align:center;background:#0d1117;border-radius:4px;">Chart not yet generated — run analysis engine</div>'}
          {f'<p style="color:#8b949e;font-size:13px;margin-top:10px;">{note}</p>' if note else ''}
        </div>"""

    return f"""
    <section id="stock-charts">
      <h2>Individual Stock Charts</h2>
      <div style="margin-top:16px;">{cards}</div>
    </section>"""


def _section_stock_discovery(discovery):
    if not discovery:
        return ""

    matches = discovery.get("matches", [])
    top_ranked = discovery.get("top_ranked", [])
    sector_rankings = discovery.get("sector_rankings", [])
    summary = html.escape(str(discovery.get("summary", "")))

    match_cards = ""
    for item in matches[:10]:
        ticker = html.escape(str(item.get("ticker", "")))
        score_data = item.get("score", {})
        final_score = score_data.get("final_score", 0)
        rating = html.escape(str(score_data.get("rating", "N/A")))
        why = item.get("why_now", {})
        tech = item.get("technical_screen", {})
        probability = item.get("probability", {})
        report_text = item.get("report", "")
        bull = ""
        bear = ""
        if report_text:
            for line in report_text.split("\n"):
                if line.startswith("Bull Case:"):
                    bull = html.escape(line.replace("Bull Case:", "").strip())
                elif line.startswith("Bear Case:"):
                    bear = html.escape(line.replace("Bear Case:", "").strip())

        match_cards += f"""
        <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;margin-bottom:12px;">
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <span style="color:#e6edf3;font-size:16px;font-weight:700;">{ticker}</span>
            <div style="display:flex;gap:8px;align-items:center;">
              <span style="color:{_score_color(final_score)};font-weight:700;">{final_score}/100</span>
              {_score_badge(final_score, rating)}
            </div>
          </div>
          <div style="background:#0d1117;border-radius:4px;padding:8px;margin-top:8px;"><span style="color:#58a6ff;font-size:12px;font-weight:700;">PROBABILITY: </span><span style="color:#e6edf3;font-size:13px;">{_fmt(probability.get('probability_pct'), 1)}% outperformance | Confidence: {html.escape(str(probability.get('confidence','N/A')))}</span></div>
          {f'<div style="background:#1a2840;border-radius:4px;padding:8px;margin-top:8px;"><span style="color:#58a6ff;font-size:12px;font-weight:700;">WHY NOW: </span><span style="color:#e6edf3;font-size:13px;">{html.escape(str(why.get("reason","")))} — {html.escape(str(why.get("evidence","")))} </span></div>' if why.get("send_alert") else ""}
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px;">
            {f'<div style="background:#0a2a14;border-radius:4px;padding:8px;font-size:12px;"><span style="color:#2ea043;font-weight:700;">▲ BULL: </span><span style="color:#8b949e;">{bull}</span></div>' if bull else ""}
            {f'<div style="background:#2a0a0a;border-radius:4px;padding:8px;font-size:12px;"><span style="color:#da3633;font-weight:700;">▼ BEAR: </span><span style="color:#8b949e;">{bear}</span></div>' if bear else ""}
          </div>
          {f'<div style="color:#6e7681;font-size:12px;margin-top:6px;">Setup: {html.escape(str(tech.get("setup_type","")))} </div>' if tech.get("setup_type") else ""}
        </div>"""

    # Sector table
    sector_html = ""
    if sector_rankings:
        rows = ""
        for s in sector_rankings[:8]:
            rows += f"<tr><td>{html.escape(str(s.get('sector','')))}</td><td>{_fmt(s.get('average_score'),0)}</td><td>{s.get('leader_count',0)}</td></tr>"
        sector_html = f"""
        <div style="margin-top:20px;">
          <div style="color:#8b949e;font-size:12px;margin-bottom:8px;letter-spacing:1px;">SECTOR INTELLIGENCE RANKINGS</div>
          <table style="font-size:13px;">
            <thead><tr><th>Sector</th><th>Avg Score</th><th>Stocks Tracked</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>"""

    return f"""
    <section id="stock-discovery">
      <h2>Stock Discovery</h2>
      <p style="color:#8b949e;">{summary}</p>
      <div style="margin-top:16px;">
        <div style="color:#8b949e;font-size:12px;margin-bottom:10px;letter-spacing:1px;">SCREENER MATCHES</div>
        {match_cards or '<p style="color:#6e7681;">No stocks passed the active screener this run.</p>'}
      </div>
      {sector_html}
    </section>"""


def _section_research_notes(rows):
    notes = [r for r in rows if r.get("research_note")]
    if not notes:
        return ""
    cards = ""
    for row in notes:
        ticker = html.escape(str(row.get("ticker", "")))
        note = html.escape(str(row.get("research_note", "")))
        score = row.get("quant_score") or row.get("score") or 0
        cards += f"""
        <div style="border-left:3px solid {_score_color(score)};padding:10px 14px;margin:10px 0;background:#161b22;border-radius:0 6px 6px 0;">
          <div style="color:#e6edf3;font-weight:700;margin-bottom:4px;">{ticker} {_score_badge(score)}</div>
          <p style="color:#8b949e;margin:0;font-size:13px;">{note}</p>
        </div>"""
    return f"""
    <section id="research-notes">
      <h2>AI Research Notes</h2>
      {cards}
    </section>"""




def _section_watchlist_intelligence(payload):
    items = payload.get("items", []) if payload else []
    if not items:
        return ""
    rows = ""
    for item in items[:20]:
        flags = ", ".join(item.get("flags", [])) or "OK"
        rows += f"""<tr>
          <td style="font-weight:700;color:#e6edf3;">{html.escape(str(item.get('ticker','')))}</td>
          <td>{html.escape(str(item.get('thesis','N/A')))}</td>
          <td>{html.escape(str(item.get('entry_zone','N/A')))}</td>
          <td>{html.escape(str(item.get('stop_loss','N/A')))}</td>
          <td>{html.escape(str(item.get('target_price','N/A')))}</td>
          <td>{html.escape(str(item.get('time_horizon','N/A')))}</td>
          <td>{html.escape(flags)}</td>
        </tr>"""
    return f"""
    <section id="watchlist-intelligence">
      <h2>Watchlist Intelligence</h2>
      <p>{html.escape(str(payload.get('summary','')))}</p>
      <div style="overflow-x:auto;margin-top:16px;">
        <table><thead><tr><th>Ticker</th><th>Thesis</th><th>Entry</th><th>Stop</th><th>Target</th><th>Horizon</th><th>Status</th></tr></thead><tbody>{rows}</tbody></table>
      </div>
    </section>"""



def _section_signal_performance(payload):
    if not payload:
        return ""
    if payload.get("message"):
        return f"""
    <section id="signal-performance">
      <h2>Signal Expected Value</h2>
      <p>{html.escape(str(payload.get('message')))}</p>
    </section>"""
    rows = ""
    for signal_type, item in payload.get("signal_types", {}).items():
        ev = item.get("expected_value")
        variance = item.get("variance")
        p_value = item.get("p_value")
        attr = html.escape(str(item.get("attractiveness", "N/A")))
        ci = item.get("confidence_interval_95", {})
        train_test = item.get("train_test", {})
        ci_text = f"{_pct(ci.get('low'), multiply=True)} to {_pct(ci.get('high'), multiply=True)}"
        test_text = _pct(train_test.get("test_average_return"), multiply=True) if train_test.get("available") else "N/A"
        color = "#2ea043" if "attractive" in attr.lower() or attr == "Constructive" else "#d29922" if ev and ev > 0 else "#da3633"
        rows += f"""<tr>
          <td style="font-weight:700;color:#e6edf3;">{html.escape(str(signal_type))}</td>
          <td>{item.get('sample_size', 0)}</td>
          <td>{_pct(item.get('win_rate'), multiply=True)}</td>
          <td style="color:#2ea043;">{_pct(item.get('average_win'), multiply=True)}</td>
          <td style="color:#da3633;">{_pct(item.get('average_loss'), multiply=True)}</td>
          <td style="color:{color};font-weight:700;">{_pct(ev, multiply=True)}</td>
          <td>{ci_text}</td>
          <td>{_fmt(variance, 4)}</td>
          <td>{_pct(item.get('standard_deviation'), multiply=True)}</td>
          <td>{_fmt(item.get('sharpe_like'), 2)}</td>
          <td>{_fmt(p_value, 4)}</td>
          <td>{test_text}</td>
          <td style="color:{color};font-weight:700;">{attr}</td>
        </tr>"""
    regression = payload.get("regression", {})
    regression_note = regression.get("message") if not regression.get("available") else f"Regression sample size {regression.get('sample_size')} | R? {_fmt(regression.get('r_squared'), 3)}"
    return f"""
    <section id="signal-performance">
      <h2>Signal Expected Value</h2>
      <p>{html.escape(str(payload.get('interpretation','')))}</p>
      <div style="overflow-x:auto;margin-top:16px;"><table><thead><tr><th>Signal</th><th>N</th><th>Win Rate</th><th>Avg Win</th><th>Avg Loss</th><th>EV</th><th>95% CI</th><th>Variance</th><th>Std Dev</th><th>Sharpe-like</th><th>p-value</th><th>Test Avg</th><th>Read</th></tr></thead><tbody>{rows or '<tr><td colspan="13">No completed signal outcomes yet.</td></tr>'}</tbody></table></div>
      <p style="font-size:12px;color:#8b949e;">{html.escape(str(regression_note or ''))}</p>
    </section>"""

def _section_trade_journal(payload):
    if not payload:
        return ""
    rows = ""
    for item in payload.get("closed_trades", [])[-10:]:
        pnl = item.get("pnl", 0)
        color = "#2ea043" if pnl >= 0 else "#da3633"
        rows += f"""<tr>
          <td>{html.escape(str(item.get('ticker','')))}</td>
          <td>{_fmt(item.get('shares'), 2)}</td>
          <td>${_fmt(item.get('entry_price'), 2)}</td>
          <td>${_fmt(item.get('exit_price'), 2)}</td>
          <td style="color:{color};font-weight:700;">${_fmt(pnl, 2)}</td>
          <td>{html.escape(str(item.get('entry_reason','')))}</td>
          <td>{html.escape(str(item.get('exit_reason','')))}</td>
        </tr>"""
    return f"""
    <section id="trade-journal">
      <h2>Trade Journal</h2>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-top:12px;">
        <div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px;"><div style="color:#8b949e;font-size:12px;">Closed Trades</div><div style="color:#e6edf3;font-size:20px;font-weight:700;">{payload.get('closed_trade_count',0)}</div></div>
        <div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px;"><div style="color:#8b949e;font-size:12px;">Realized P&L</div><div style="color:{'#2ea043' if payload.get('realized_pnl',0) >= 0 else '#da3633'};font-size:20px;font-weight:700;">${_fmt(payload.get('realized_pnl'),2)}</div></div>
        <div style="background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px;"><div style="color:#8b949e;font-size:12px;">Win Rate</div><div style="color:#58a6ff;font-size:20px;font-weight:700;">{_pct(payload.get('win_rate'), multiply=True)}</div></div>
      </div>
      <div style="overflow-x:auto;margin-top:16px;"><table><thead><tr><th>Ticker</th><th>Shares</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Entry Reason</th><th>Exit Reason</th></tr></thead><tbody>{rows or '<tr><td colspan="7">No closed trades logged yet.</td></tr>'}</tbody></table></div>
    </section>"""


def _section_earnings_alerts(payload):
    if not payload:
        return ""
    rows = ""
    for item in (payload.get("alerts") or payload.get("upcoming") or [])[:12]:
        rows += f"""<tr>
          <td style="font-weight:700;color:#e6edf3;">{html.escape(str(item.get('ticker','')))}</td>
          <td>{html.escape(str(item.get('next_earnings_date','N/A')))}</td>
          <td>{html.escape(str(item.get('days_until','N/A')))}</td>
          <td>{_fmt(item.get('score'), 1)}</td>
          <td>{html.escape(str(item.get('rating','N/A')))}</td>
          <td>{html.escape(str(item.get('expected_move','Unavailable')))}</td>
        </tr>"""
    return f"""
    <section id="earnings-alerts">
      <h2>Earnings Calendar</h2>
      <p>{html.escape(str(payload.get('summary','')))}</p>
      <div style="overflow-x:auto;margin-top:16px;"><table><thead><tr><th>Ticker</th><th>Date</th><th>Days</th><th>Score</th><th>Rating</th><th>Expected Move</th></tr></thead><tbody>{rows or '<tr><td colspan="6">No upcoming earnings data available.</td></tr>'}</tbody></table></div>
    </section>"""


def _section_benchmark_and_drift(portfolio):
    if not portfolio:
        return ""
    bench = portfolio.get("benchmark_comparison", {})
    drift = portfolio.get("drift_monitor", {})
    period_rows = ""
    for label, item in bench.get("periods", {}).items():
        rel = item.get("relative_return")
        color = "#2ea043" if (rel or 0) >= 0 else "#da3633"
        period_rows += f"<tr><td>{label}</td><td>{_pct(item.get('portfolio_return'), multiply=True)}</td><td>{_pct(item.get('benchmark_return'), multiply=True)}</td><td style='color:{color};font-weight:700;'>{_pct(rel, multiply=True)}</td></tr>"
    drift_rows = ""
    for item in drift.get("positions", [])[:12]:
        color = "#d29922" if item.get("status") == "rebalance_watch" else "#2ea043"
        drift_rows += f"<tr><td>{html.escape(str(item.get('ticker','')))}</td><td>{_fmt(item.get('current_weight_pct'),2)}%</td><td>{_fmt(item.get('target_weight_pct'),2)}%</td><td style='color:{color};font-weight:700;'>{_fmt(item.get('drift_pct'),2)}%</td><td>{html.escape(str(item.get('status','')))}</td></tr>"
    return f"""
    <section id="benchmark-drift">
      <h2>Benchmark & Drift</h2>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:16px;">
        <div><div style="color:#8b949e;font-size:12px;margin-bottom:8px;letter-spacing:1px;">PORTFOLIO VS SPY</div><table><thead><tr><th>Period</th><th>Portfolio</th><th>SPY</th><th>Relative</th></tr></thead><tbody>{period_rows or '<tr><td colspan="4">Benchmark comparison unavailable.</td></tr>'}</tbody></table></div>
        <div><div style="color:#8b949e;font-size:12px;margin-bottom:8px;letter-spacing:1px;">TARGET WEIGHT DRIFT</div><table><thead><tr><th>Ticker</th><th>Current</th><th>Target</th><th>Drift</th><th>Status</th></tr></thead><tbody>{drift_rows or '<tr><td colspan="5">Drift monitor unavailable.</td></tr>'}</tbody></table></div>
      </div>
    </section>"""


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate_dashboard(
    quant_path=os.path.join(STATE_DIR, "latest_quant_research.json"),
    portfolio_path=os.path.join(STATE_DIR, "latest_portfolio_report.json"),
    discovery_path=os.path.join(STATE_DIR, "latest_stock_discovery.json"),
    comparison_path=os.path.join(STATE_DIR, "latest_comparison.json"),
    output_path=DASHBOARD_FILE,
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    quant = _load_json(quant_path, {"tickers": [], "summary": ""})
    portfolio = _load_json(portfolio_path, {})
    discovery = _load_json(discovery_path, {})
    comparison = _load_json(comparison_path, {"tickers": []})
    watchlist_payload = _load_json(os.path.join(STATE_DIR, "latest_watchlist_intelligence.json"), {})
    trade_payload = _load_json(os.path.join(STATE_DIR, "latest_trade_journal.json"), {})
    earnings_payload = _load_json(os.path.join(STATE_DIR, "latest_earnings_alerts.json"), {})
    signal_payload = _load_json(os.path.join(STATE_DIR, "latest_signal_performance.json"), {})

    rows = quant.get("tickers") or comparison.get("tickers", [])

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    # Build all sections
    sec_overview = _section_portfolio_overview(portfolio)
    sec_exec_chart = _section_executive_chart()
    sec_risk = _section_portfolio_risk(portfolio)
    sec_benchmark_drift = _section_benchmark_and_drift(portfolio)
    sec_watchlist = _section_watchlist(rows)
    sec_watchlist_intel = _section_watchlist_intelligence(watchlist_payload)
    sec_charts = _section_stock_charts(rows)
    sec_earnings = _section_earnings_alerts(earnings_payload)
    sec_discovery = _section_stock_discovery(discovery)
    sec_trade = _section_trade_journal(trade_payload)
    sec_signal_perf = _section_signal_performance(signal_payload)
    sec_notes = _section_research_notes(rows)

    nav_links = "".join(
        f"<a href='#{anchor}'>{label}</a>"
        for label, anchor in [
            ("Portfolio", "portfolio-overview"),
            ("Risk", "portfolio-risk"),
            ("Benchmark", "benchmark-drift"),
            ("Watchlist", "watchlist"),
            ("Thesis", "watchlist-intelligence"),
            ("Charts", "stock-charts"),
            ("Earnings", "earnings-alerts"),
            ("Discovery", "stock-discovery"),
            ("Journal", "trade-journal"),
            ("Signal EV", "signal-performance"),
            ("Research", "research-notes"),
        ]
    )

    html_out = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>JFO Quant Intelligence Dashboard</title>
  <style>
    *{{box-sizing:border-box;}}
    :root{{--bg:#0d1117;--surface:#161b22;--border:#30363d;--text:#e6edf3;--muted:#8b949e;--accent:#238636;--blue:#58a6ff;}}
    body{{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--text);}}
    header{{padding:20px 32px 16px;border-bottom:1px solid var(--border);background:var(--surface);}}
    header h1{{margin:0 0 4px;font-size:22px;font-weight:700;}}
    header p{{margin:0;color:var(--muted);font-size:13px;}}
    nav{{display:flex;gap:6px;padding:10px 32px;border-bottom:1px solid var(--border);background:var(--surface);position:sticky;top:0;z-index:100;flex-wrap:wrap;}}
    nav a{{color:var(--blue);text-decoration:none;font-size:13px;border:1px solid var(--border);padding:5px 10px;border-radius:20px;transition:background 0.15s;}}
    nav a:hover{{background:#21262d;}}
    main{{padding:24px 32px 48px;max-width:1200px;margin:0 auto;}}
    section{{margin-bottom:48px;}}
    h2{{color:var(--text);font-size:18px;font-weight:700;margin:0 0 4px;padding-bottom:8px;border-bottom:1px solid var(--border);}}
    p{{color:var(--muted);line-height:1.6;margin:8px 0;}}
    table{{width:100%;border-collapse:collapse;font-size:13px;}}
    th{{background:#21262d;color:var(--muted);font-weight:600;padding:10px 8px;text-align:left;border-bottom:1px solid var(--border);font-size:11px;letter-spacing:0.5px;text-transform:uppercase;}}
    td{{padding:10px 8px;border-bottom:1px solid #21262d;color:var(--text);}}
    tr:hover td{{background:#21262d;}}
    code{{background:#21262d;color:var(--blue);padding:2px 6px;border-radius:4px;font-size:12px;}}
    .disclaimer{{background:#161b22;border:1px solid var(--border);border-radius:6px;padding:12px;color:var(--muted);font-size:12px;margin-top:32px;}}
    @media (max-width: 760px){{header{{padding:16px;}}nav{{padding:8px 16px;}}main{{padding:16px;}}section{{margin-bottom:32px;}}table{{display:block;overflow-x:auto;white-space:nowrap;}}}}
  </style>
</head>
<body>
  <header>
    <h1>Jain Family Office — Quant Intelligence Engine</h1>
    <p>Generated {generated_at} &nbsp;|&nbsp; Educational research analytics only, not financial advice.</p>
  </header>
  <nav>{nav_links}</nav>
  <main>
    {sec_overview}
    {sec_exec_chart}
    {sec_risk}
    {sec_benchmark_drift}
    {sec_watchlist}
    {sec_watchlist_intel}
    {sec_charts}
    {sec_earnings}
    {sec_discovery}
    {sec_trade}
    {sec_signal_perf}
    {sec_notes}
    <div class="disclaimer">
      ⚠ This dashboard is for educational and research purposes only. Nothing here constitutes financial advice,
      a recommendation to buy or sell any security, or a solicitation of any investment. All data is sourced
      from public APIs and may be delayed or inaccurate. Past performance does not guarantee future results.
    </div>
  </main>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_out)

    return output_path
