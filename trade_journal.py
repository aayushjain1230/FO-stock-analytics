import json
import os
from datetime import datetime
from typing import Dict, List

TRADE_FILE = "trade_journal.json"
STATE_FILE = os.path.join("state", "latest_trade_journal.json")


def load_trades(path: str = TRADE_FILE) -> List[Dict]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload.get("trades", payload if isinstance(payload, list) else [])
    return []


def save_trades(trades: List[Dict], path: str = TRADE_FILE):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"trades": trades}, f, indent=2, default=str)


def log_trade(ticker: str, action: str, shares: float, price: float, reason: str = "", trade_date: str = None, path: str = TRADE_FILE) -> Dict:
    trades = load_trades(path)
    trade = {
        "id": len(trades) + 1,
        "ticker": ticker.upper(),
        "action": action.lower(),
        "shares": float(shares),
        "price": float(price),
        "trade_date": trade_date or datetime.now().date().isoformat(),
        "reason": reason,
        "created_at": datetime.now().isoformat(),
    }
    trades.append(trade)
    save_trades(trades, path)
    return trade


def summarize_trades(path: str = TRADE_FILE) -> Dict:
    trades = load_trades(path)
    realized = []
    open_lots = {}
    for trade in trades:
        ticker = trade.get("ticker")
        action = trade.get("action")
        shares = float(trade.get("shares") or 0)
        price = float(trade.get("price") or 0)
        if action in {"buy", "entry"}:
            open_lots.setdefault(ticker, []).append({"shares": shares, "price": price, "reason": trade.get("reason"), "date": trade.get("trade_date")})
        elif action in {"sell", "exit"}:
            remaining = shares
            lots = open_lots.setdefault(ticker, [])
            while remaining > 0 and lots:
                lot = lots[0]
                matched = min(remaining, lot["shares"])
                pnl = (price - lot["price"]) * matched
                realized.append({"ticker": ticker, "shares": matched, "entry_price": lot["price"], "exit_price": price, "pnl": pnl, "entry_reason": lot.get("reason"), "exit_reason": trade.get("reason")})
                lot["shares"] -= matched
                remaining -= matched
                if lot["shares"] <= 0:
                    lots.pop(0)
    wins = [item for item in realized if item["pnl"] > 0]
    total_pnl = sum(item["pnl"] for item in realized)
    payload = {
        "generated_at": datetime.now().isoformat(),
        "trade_count": len(trades),
        "closed_trade_count": len(realized),
        "open_positions": {ticker: sum(lot["shares"] for lot in lots) for ticker, lots in open_lots.items() if sum(lot["shares"] for lot in lots) > 0},
        "realized_pnl": round(total_pnl, 2),
        "win_rate": round(len(wins) / len(realized), 4) if realized else None,
        "closed_trades": realized[-20:],
        "summary": f"{len(realized)} closed journal entries; realized P&L ${total_pnl:.2f}.",
    }
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return payload
