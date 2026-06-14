import json
import os
from datetime import datetime
from typing import Dict, Iterable, List

WATCHLIST_INTEL_FILE = "watchlist_intelligence.json"
STATE_FILE = os.path.join("state", "latest_watchlist_intelligence.json")


def load_watchlist_intelligence(path: str = WATCHLIST_INTEL_FILE) -> Dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {"tickers": {}}
    return {"tickers": {}}


def save_watchlist_intelligence(payload: Dict, path: str = WATCHLIST_INTEL_FILE):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)


def ensure_watchlist_records(watchlist: Iterable[str], path: str = WATCHLIST_INTEL_FILE) -> Dict:
    payload = load_watchlist_intelligence(path)
    tickers = payload.setdefault("tickers", {})
    changed = False
    for ticker in [item.upper() for item in watchlist]:
        if ticker not in tickers:
            tickers[ticker] = {
                "ticker": ticker,
                "reason_added": "Research watchlist candidate. Add thesis before acting.",
                "thesis": "Thesis not set yet.",
                "entry_zone": None,
                "stop_loss": None,
                "target_price": None,
                "time_horizon": "Unspecified",
                "risk_budget_pct": None,
                "date_added": datetime.now().date().isoformat(),
                "status": "watching",
                "what_would_change_my_mind": "Define invalidation criteria.",
            }
            changed = True
    stale = [ticker for ticker in tickers if ticker not in {item.upper() for item in watchlist}]
    for ticker in stale:
        tickers[ticker]["status"] = "removed_from_active_watchlist"
        changed = True
    if changed:
        save_watchlist_intelligence(payload, path)
    return payload


def build_watchlist_report(watchlist: Iterable[str], quant_rows: List[Dict] = None, path: str = WATCHLIST_INTEL_FILE) -> Dict:
    payload = ensure_watchlist_records(watchlist, path)
    row_map = {row.get("ticker"): row for row in (quant_rows or [])}
    rows = []
    missing_thesis = []
    alert_triggers = []
    for ticker in [item.upper() for item in watchlist]:
        meta = payload.get("tickers", {}).get(ticker, {})
        quant = row_map.get(ticker, {})
        close = quant.get("close")
        target = meta.get("target_price")
        stop = meta.get("stop_loss")
        flags = []
        if not meta.get("thesis") or meta.get("thesis") == "Thesis not set yet.":
            flags.append("missing thesis")
            missing_thesis.append(ticker)
        if stop and close and close <= stop:
            flags.append("stop level reached")
            alert_triggers.append(f"{ticker} reached or broke stop level")
        if target and close and close >= target:
            flags.append("target reached")
            alert_triggers.append(f"{ticker} reached or exceeded target")
        rows.append({**meta, "current_price": close, "score": quant.get("quant_score") or quant.get("score"), "rating": quant.get("quant_label") or quant.get("rating"), "flags": flags})
    report = {
        "generated_at": datetime.now().isoformat(),
        "items": rows,
        "missing_thesis": missing_thesis,
        "alert_triggers": alert_triggers,
        "summary": f"{len(rows)} watchlist names tracked; {len(missing_thesis)} need a written thesis.",
    }
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    return report
