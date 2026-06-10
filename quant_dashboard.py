import html
import json
import os
from datetime import datetime


STATE_DIR = "state"
PLOTS_DIR = "plots"
DASHBOARD_FILE = os.path.join(PLOTS_DIR, "quant_research_dashboard.html")


def _load_json(path, fallback):
    if not os.path.exists(path):
        return fallback
    with open(path, "r") as f:
        return json.load(f)


def _fmt(value, precision=2):
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.{precision}f}"
    except Exception:
        return html.escape(str(value))


def _pct(value):
    if value is None:
        return "N/A"
    try:
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return html.escape(str(value))


def _table(rows):
    if not rows:
        return "<p>No watchlist data available. Run <code>python main.py --quant-report</code>.</p>"
    body = []
    for row in rows:
        body.append(
            "<tr>"
            f"<td>{html.escape(str(row.get('ticker', 'N/A')))}</td>"
            f"<td>{_fmt(row.get('quant_score') or row.get('score'), 0)}</td>"
            f"<td>{html.escape(str(row.get('quant_label') or row.get('rating', 'N/A')))}</td>"
            f"<td>{_fmt(row.get('close'))}</td>"
            f"<td>{_fmt(row.get('sharpe_ratio'))}</td>"
            f"<td>{_pct(row.get('annualized_volatility'))}</td>"
            f"<td>{_pct(row.get('max_drawdown'))}</td>"
            f"<td>{html.escape(str(row.get('volatility_regime', 'N/A')))}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>Ticker</th><th>Quant Score</th><th>Label</th><th>Close</th>"
        "<th>Sharpe</th><th>Ann. Vol</th><th>Max DD</th><th>Vol Regime</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def generate_dashboard(
    quant_path=os.path.join(STATE_DIR, "latest_quant_research.json"),
    comparison_path=os.path.join(STATE_DIR, "latest_comparison.json"),
    output_path=DASHBOARD_FILE,
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    quant = _load_json(quant_path, {"tickers": [], "summary": "No quant report available."})
    comparison = _load_json(comparison_path, {"tickers": []})
    rows = quant.get("tickers") or comparison.get("tickers", [])
    notes = [
        f"<article><h3>{html.escape(str(row.get('ticker', 'N/A')))}</h3><p>{html.escape(str(row.get('research_note', 'No research note available.')))}</p></article>"
        for row in rows
        if row.get("research_note")
    ]
    nav = [
        "Market Overview",
        "Watchlist",
        "Technical Analysis",
        "Momentum Research",
        "Options Lab",
        "Greeks Lab",
        "Monte Carlo Simulator",
        "Statistical Arbitrage",
        "Portfolio Analytics",
        "Risk Analytics",
    ]
    nav_html = "".join(f"<a href='#{html.escape(item.lower().replace(' ', '-'))}'>{html.escape(item)}</a>" for item in nav)
    content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>JFO Quant Research Dashboard</title>
  <style>
    :root {{ color-scheme: light; --ink:#17202a; --muted:#5d6d7e; --line:#d7dbdd; --panel:#f8f9f9; --accent:#117a65; }}
    body {{ margin:0; font-family:Arial, sans-serif; color:var(--ink); background:#ffffff; }}
    header {{ padding:24px 32px 12px; border-bottom:1px solid var(--line); }}
    h1 {{ margin:0 0 8px; font-size:28px; letter-spacing:0; }}
    h2 {{ margin:26px 0 10px; font-size:20px; }}
    h3 {{ margin:0 0 6px; font-size:15px; }}
    p {{ color:var(--muted); line-height:1.45; }}
    nav {{ display:flex; gap:8px; overflow-x:auto; padding:12px 32px; border-bottom:1px solid var(--line); }}
    nav a {{ color:var(--accent); text-decoration:none; white-space:nowrap; font-size:13px; border:1px solid var(--line); padding:6px 8px; border-radius:4px; }}
    main {{ padding:0 32px 36px; max-width:1180px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th, td {{ border-bottom:1px solid var(--line); padding:9px 8px; text-align:left; }}
    th {{ background:var(--panel); font-weight:700; }}
    article {{ border-left:3px solid var(--accent); padding:10px 12px; margin:10px 0; background:var(--panel); }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(240px, 1fr)); gap:14px; }}
    .metric {{ background:var(--panel); border:1px solid var(--line); border-radius:6px; padding:14px; }}
    code {{ background:#eef2f3; padding:2px 4px; border-radius:3px; }}
  </style>
</head>
<body>
  <header>
    <h1>Jain Family Office Quant Intelligence Engine</h1>
    <p>Generated {html.escape(datetime.now().isoformat(timespec='seconds'))}. Educational research analytics only, not financial advice.</p>
  </header>
  <nav>{nav_html}</nav>
  <main>
    <section id="market-overview">
      <h2>Market Overview</h2>
      <p>{html.escape(str(quant.get('summary', 'Run the quant report to populate this section.')))}</p>
    </section>
    <section id="watchlist"><h2>Watchlist</h2>{_table(rows)}</section>
    <section id="technical-analysis"><h2>Technical Analysis</h2><p>Price, moving-average, RSI, MACD, rate-of-change, relative strength, and volume metrics are calculated in the analytics engine.</p></section>
    <section id="momentum-research"><h2>Momentum Research</h2><p>Use <code>backtesting.run_momentum_research</code> to test 3, 6, 12, and 24 month momentum portfolios with costs and slippage.</p></section>
    <section id="options-lab"><h2>Options Lab</h2><p>Run <code>python main.py --option-lab --stock-price 195 --strike 195 --days 37 --volatility 0.31</code>.</p></section>
    <section id="greeks-lab"><h2>Greeks Lab</h2><p>Delta, gamma, vega, theta, and rho are generated by the options engine with plain-English explanations.</p></section>
    <section id="monte-carlo-simulator"><h2>Monte Carlo Simulator</h2><p>The options engine simulates terminal stock prices and compares Monte Carlo option value with Black-Scholes fair value.</p></section>
    <section id="statistical-arbitrage"><h2>Statistical Arbitrage</h2><p>Pairs trading utilities calculate correlation, spread, z-score, signal state, and backtest performance.</p></section>
    <section id="portfolio-analytics"><h2>Portfolio Analytics</h2><p>Portfolio beta, volatility, correlation matrix, diversification score, and concentration risk are available in <code>quant_analytics.portfolio_analytics</code>.</p></section>
    <section id="risk-analytics"><h2>Risk Analytics</h2><p>VaR, expected shortfall, stress-test inputs, drawdowns, and volatility targeting are available in the quant modules.</p></section>
    <section><h2>AI Research Analyst</h2>{''.join(notes) if notes else '<p>No research notes generated yet.</p>'}</section>
  </main>
</body>
</html>"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    return output_path
