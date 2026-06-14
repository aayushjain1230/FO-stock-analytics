import json
import os
from datetime import datetime, date
from typing import Dict, Iterable, List

STATE_FILE = os.path.join("state", "latest_earnings_alerts.json")


def _parse_date(value):
    if not value:
        return None
    text = str(value).split(" ")[0]
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        return None


def build_earnings_alerts(stock_reports: Iterable[Dict], days_ahead: int = 2) -> Dict:
    today = date.today()
    alerts = []
    upcoming = []
    for report in stock_reports:
        ticker = report.get("ticker")
        earnings = report.get("earnings", {})
        fundamentals = report.get("fundamentals", {})
        next_date = _parse_date(earnings.get("next_earnings_date") or fundamentals.get("next_earnings_date"))
        if not ticker or not next_date:
            continue
        days_until = (next_date - today).days
        item = {
            "ticker": ticker,
            "next_earnings_date": next_date.isoformat(),
            "days_until": days_until,
            "score": report.get("score", {}).get("final_score"),
            "rating": report.get("score", {}).get("rating"),
            "earnings_risk_score": earnings.get("risk_score"),
            "last_surprise_pct": earnings.get("last_surprise_pct"),
            "beat_streak": earnings.get("beat_streak"),
            "expected_move": "Unavailable until options IV scan is run.",
        }
        if 0 <= days_until <= days_ahead:
            alerts.append(item)
        if days_until >= 0:
            upcoming.append(item)
    payload = {
        "generated_at": datetime.now().isoformat(),
        "days_ahead": days_ahead,
        "alerts": sorted(alerts, key=lambda x: x["days_until"]),
        "upcoming": sorted(upcoming, key=lambda x: x["days_until"])[:20],
        "summary": f"{len(alerts)} watchlist earnings events within {days_ahead} days.",
    }
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return payload


def format_telegram(payload: Dict) -> str:
    if not payload.get("alerts"):
        return ""
    lines = ["EARNINGS WATCH", "", payload.get("summary", "")]
    for item in payload.get("alerts", []):
        lines.extend([
            "",
            f"{item['ticker']} reports in {item['days_until']} day(s) ({item['next_earnings_date']})",
            f"Score: {item.get('score', 'N/A')} | Rating: {item.get('rating', 'N/A')}",
            f"Earnings risk: {item.get('earnings_risk_score', 'N/A')}",
            f"Expected move: {item.get('expected_move', 'Unavailable')}",
            "What to watch: gap reaction, volume, guidance, and post-earnings drift.",
        ])
    lines.append("\nEducational research only, not financial advice.")
    return "\n".join(lines)
