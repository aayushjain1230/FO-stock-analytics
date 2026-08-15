"""Version 2 CLI orchestrator for Watchlist Intelligence.

Advanced research modules remain in the repository for later phases, but this
entry point intentionally imports only the reliable watchlist surface at
startup. Optional/network-heavy dependencies are imported inside services.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.services.configuration import load_config
from app.services.watchlist import add_ticker, load_cli_watchlist, remove_ticker, save_cli_watchlist


def list_watchlist() -> None:
    """Print the local CLI watchlist."""
    print("Current Active Watchlist: " + ", ".join(load_cli_watchlist()))


def update_watchlist(add: list[str] | None = None, remove: list[str] | None = None) -> None:
    """Apply CLI watchlist edits to the local watchlist file."""
    tickers = load_cli_watchlist()
    for ticker in add or []:
        tickers, error = add_ticker(tickers, ticker)
        print(error or f"Added {ticker.upper()}")
    for ticker in remove or []:
        tickers = remove_ticker(tickers, ticker)
        print(f"Removed {ticker.upper()}")
    save_cli_watchlist(tickers)


def analyze_watchlist(send_telegram: bool = False, dry_run_telegram: bool = False) -> dict:
    """Analyze the CLI watchlist and optionally send a daily Telegram brief."""
    from app.services.analysis import analyze_stock, daily_summary
    from app.services.change_detection import detect_changes, load_previous_snapshot, save_snapshot
    from app.services.market_data import fetch_market_snapshot, fetch_price_history, fetch_snapshots
    from app.services.telegram import render_daily_brief, send_daily_brief
    from app.services.v2_intelligence import build_v2_intelligence

    config = load_config()
    tickers = load_cli_watchlist()
    market = fetch_market_snapshot()
    histories = fetch_price_history(tickers, period=config.period, interval=config.interval)
    snapshots = fetch_snapshots(tickers, period=config.period, interval=config.interval)
    analyses = [analyze_stock(snapshots[ticker], histories.get(ticker)) for ticker in tickers]
    v2_intelligence = build_v2_intelligence(analyses, histories, market_condition=market.condition, sync_sec=False)
    previous = load_previous_snapshot()
    changes = detect_changes(analyses, previous)
    save_snapshot(analyses)
    brief = render_daily_brief(market, analyses, v2_intelligence)
    if send_telegram:
        result = send_daily_brief(brief, dry_run=dry_run_telegram)
        print(json.dumps(result, indent=2))
    payload = {
        "market": market.__dict__,
        "summary": daily_summary(analyses),
        "changes": changes,
        "analyses": [item.to_dict() for item in analyses],
        "v2_intelligence": {
            ticker: {
                "whale_activity": item.whale_activity.to_dict(),
                "simulation": item.simulation.to_dict(),
                "filing_insights": [insight.to_dict() for insight in item.filing_insights],
                "relative_values": [pair.to_dict() for pair in item.relative_values],
                "volatility": item.volatility.to_dict(),
                "factor_model": item.factor_model.to_dict(),
                "risk_metrics": item.risk_metrics.to_dict(),
            }
            for ticker, item in v2_intelligence.items()
        },
        "telegram_brief": brief,
    }
    Path("state").mkdir(exist_ok=True)
    Path("state/v2_latest_analysis.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(payload["summary"])
    return payload


def generate_dashboard_placeholder() -> None:
    """Keep --dashboard useful without exposing legacy lab pages."""
    payload = analyze_watchlist(send_telegram=False)
    Path("plots").mkdir(exist_ok=True)
    rows = "".join(
        f"<tr><td>{item['ticker']}</td><td>{item['overall_view']}</td><td>{item['trend']}</td><td>{item['what_changed']}</td></tr>"
        for item in payload["analyses"]
    )
    html = f"""<!doctype html>
<html><head><meta charset='utf-8'><title>Watchlist Intelligence</title>
<style>body{{background:#070a10;color:#e7eef8;font-family:Segoe UI,sans-serif;padding:32px}}table{{width:100%;border-collapse:collapse}}td,th{{border-bottom:1px solid #233044;padding:10px;text-align:left}}.card{{background:#111827;border:1px solid #233044;border-radius:16px;padding:18px;margin:14px 0}}</style>
</head><body><h1>Watchlist Intelligence</h1><div class='card'>{payload['summary']}</div><table><thead><tr><th>Ticker</th><th>View</th><th>Trend</th><th>What changed</th></tr></thead><tbody>{rows}</tbody></table><p>Run <code>streamlit run streamlit_app.py</code> for the interactive Version 2 app.</p></body></html>"""
    Path("plots/watchlist_intelligence.html").write_text(html, encoding="utf-8")
    print("Dashboard saved to plots/watchlist_intelligence.html")


def main() -> None:
    """Parse CLI commands."""
    parser = argparse.ArgumentParser(description="Watchlist-first stock intelligence application")
    parser.add_argument("--add", nargs="+", help="Add ticker(s) to the local CLI watchlist")
    parser.add_argument("--remove", nargs="+", help="Remove ticker(s) from the local CLI watchlist")
    parser.add_argument("--list", action="store_true", help="List the local CLI watchlist")
    parser.add_argument("--analyze", action="store_true", help="Analyze the watchlist")
    parser.add_argument("--dashboard", action="store_true", help="Generate a simple V2 HTML summary")
    parser.add_argument("--send-telegram", action="store_true", help="Send the plain-English Telegram daily brief")
    parser.add_argument("--dry-run-telegram", action="store_true", help="Render Telegram output without sending")
    parser.add_argument("--model-report", action="store_true", help="Generate the private V3 model methodology report")
    args = parser.parse_args()

    if args.add or args.remove:
        update_watchlist(args.add, args.remove)
    if args.list:
        list_watchlist()
    if args.analyze:
        analyze_watchlist(send_telegram=args.send_telegram, dry_run_telegram=args.dry_run_telegram)
    if args.dashboard:
        generate_dashboard_placeholder()
    if args.model_report:
        from app.services.methodology_report import generate_model_report

        print(f"Model report saved to {generate_model_report()}")
    if not any([args.add, args.remove, args.list, args.analyze, args.dashboard, args.model_report]):
        parser.print_help()


if __name__ == "__main__":
    main()
