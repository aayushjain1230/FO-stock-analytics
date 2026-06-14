import json
import os
from datetime import datetime
from typing import Dict, Iterable

STATE_FILE = os.path.join("state", "intraday_monitor_state.json")


def _load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_state(payload):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)


def scan(yf_module, tickers: Iterable[str], price_move_pct: float = 5.0, volume_ratio: float = 2.0) -> Dict:
    state = _load_state()
    alerts = []
    new_state = {}
    for ticker in [t.upper() for t in tickers]:
        try:
            df = yf_module.download(ticker, period="5d", interval="30m", progress=False, auto_adjust=False)
            if df is None or df.empty or "Close" not in df.columns:
                continue
            latest = df.iloc[-1]
            close = float(latest["Close"])
            prev_close = float(df["Close"].dropna().iloc[-2]) if len(df.dropna(subset=["Close"])) > 1 else close
            move = (close - prev_close) / prev_close * 100 if prev_close else 0
            avg_vol = df["Volume"].tail(20).mean() if "Volume" in df.columns else 0
            vol_ratio = float(latest.get("Volume", 0) / avg_vol) if avg_vol else 0
            reasons = []
            if abs(move) >= price_move_pct:
                reasons.append(f"30m price move {move:+.2f}%")
            if vol_ratio >= volume_ratio:
                reasons.append(f"volume spike {vol_ratio:.1f}x")
            alert_key = f"{ticker}:{df.index[-1]}:{','.join(reasons)}"
            if reasons and state.get(ticker, {}).get("last_alert_key") != alert_key:
                alerts.append({"ticker": ticker, "price": close, "move_pct": move, "volume_ratio": vol_ratio, "reasons": reasons})
                new_state[ticker] = {"last_alert_key": alert_key, "last_price": close, "updated_at": datetime.now().isoformat()}
            else:
                new_state[ticker] = state.get(ticker, {"last_price": close, "updated_at": datetime.now().isoformat()})
        except Exception as exc:
            new_state[ticker] = {"error": str(exc), "updated_at": datetime.now().isoformat()}
    _save_state(new_state)
    return {"generated_at": datetime.now().isoformat(), "alerts": alerts}


def format_telegram(payload: Dict) -> str:
    if not payload.get("alerts"):
        return ""
    lines = ["INTRADAY PRICE ALERT", ""]
    for item in payload.get("alerts", []):
        lines.append(f"{item['ticker']}: ${item['price']:.2f} | {', '.join(item['reasons'])}")
    lines.append("\nWhy it matters: intraday price/volume behavior changed materially versus recent bars.")
    return "\n".join(lines)
