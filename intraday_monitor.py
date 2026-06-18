import json
import os
from datetime import datetime
from typing import Dict, Iterable

import numpy as np
import pandas as pd

STATE_FILE = os.path.join("state", "intraday_monitor_state.json")
EARNINGS_STATE_FILE = os.path.join("state", "latest_earnings_alerts.json")
PORTFOLIO_STATE_FILE = os.path.join("state", "latest_portfolio_report.json")


def _load_json(path, fallback):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return fallback
    return fallback


def _load_state():
    return _load_json(STATE_FILE, {})


def _save_state(payload):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)


def _normalize_download(df):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(-1)
    return df.dropna(subset=["Close"]) if "Close" in df.columns else pd.DataFrame()


def _session_vwap(df):
    if df.empty or not {"High", "Low", "Close", "Volume"}.issubset(df.columns):
        return pd.Series(dtype=float)
    typical = (df["High"] + df["Low"] + df["Close"]) / 3
    volume = df["Volume"].replace(0, pd.NA).fillna(0)
    return (typical * volume).cumsum() / volume.cumsum().replace(0, pd.NA)


def _ticker_reasons(df, price_move_pct, volume_ratio, breakout_lookback, z_threshold=2.0):
    latest = df.iloc[-1]
    close = float(latest["Close"])
    prev_close = float(df["Close"].dropna().iloc[-2]) if len(df.dropna(subset=["Close"])) > 1 else close
    move = (close - prev_close) / prev_close * 100 if prev_close else 0
    avg_vol = df["Volume"].tail(20).mean() if "Volume" in df.columns else 0
    vol_ratio = float(latest.get("Volume", 0) / avg_vol) if avg_vol else 0
    z_score = _return_z_score(df)
    abnormality = _abnormality_label(z_score)
    reasons = []

    if z_score is not None and abs(z_score) >= z_threshold:
        percentile = _normal_percentile(abs(z_score))
        reasons.append(f"abnormal move z-score {z_score:+.2f} ({abnormality}; larger than ~{percentile:.1f}% of normal bars)")
    if abs(move) >= price_move_pct:
        reasons.append(f"5m price move {move:+.2f}%")
    if vol_ratio >= volume_ratio:
        reasons.append(f"volume spike {vol_ratio:.1f}x")

    if len(df) > breakout_lookback:
        prior_high = float(df["High"].iloc[-breakout_lookback - 1:-1].max()) if "High" in df.columns else None
        prior_low = float(df["Low"].iloc[-breakout_lookback - 1:-1].min()) if "Low" in df.columns else None
        if prior_high and close >= prior_high:
            reasons.append(f"intraday breakout above {prior_high:.2f}")
        if prior_low and close <= prior_low:
            reasons.append(f"intraday breakdown below {prior_low:.2f}")

    sma20 = df["Close"].rolling(20).mean()
    sma50 = df["Close"].rolling(50).mean()
    if len(df) > 50 and pd.notna(sma20.iloc[-1]) and pd.notna(sma50.iloc[-1]):
        if df["Close"].iloc[-2] <= sma20.iloc[-2] and close > sma20.iloc[-1]:
            reasons.append("price reclaimed intraday SMA20")
        if df["Close"].iloc[-2] >= sma20.iloc[-2] and close < sma20.iloc[-1]:
            reasons.append("price lost intraday SMA20")
        if sma20.iloc[-2] <= sma50.iloc[-2] and sma20.iloc[-1] > sma50.iloc[-1]:
            reasons.append("intraday SMA20 crossed above SMA50")
        if sma20.iloc[-2] >= sma50.iloc[-2] and sma20.iloc[-1] < sma50.iloc[-1]:
            reasons.append("intraday SMA20 crossed below SMA50")

    vwap = _session_vwap(df.tail(78))
    if len(vwap) > 2 and pd.notna(vwap.iloc[-1]) and pd.notna(vwap.iloc[-2]):
        if df["Close"].iloc[-2] <= vwap.iloc[-2] and close > vwap.iloc[-1]:
            reasons.append("price reclaimed session VWAP")
        if df["Close"].iloc[-2] >= vwap.iloc[-2] and close < vwap.iloc[-1]:
            reasons.append("price lost session VWAP")

    return close, move, vol_ratio, z_score, abnormality, reasons


def _return_z_score(df, lookback=252):
    close = pd.to_numeric(df["Close"], errors="coerce").dropna()
    returns = close.pct_change().dropna()
    if len(returns) < 30:
        return None
    sample = returns.tail(lookback)
    current = sample.iloc[-1]
    history = sample.iloc[:-1]
    std = history.std(ddof=1)
    if not std or pd.isna(std):
        return None
    return float((current - history.mean()) / std)


def _normal_percentile(abs_z):
    return float((2 * (0.5 * (1 + np.math.erf(abs_z / np.sqrt(2)))) - 1) * 100)


def _abnormality_label(z_score):
    if z_score is None:
        return "Normal"
    magnitude = abs(z_score)
    direction = "upside" if z_score > 0 else "downside"
    if magnitude >= 3:
        return f"extremely unusual {direction} move"
    if magnitude >= 2:
        return f"unusual {direction} move"
    return "normal range"


def _earnings_alerts_for_ticker(ticker):
    payload = _load_json(EARNINGS_STATE_FILE, {})
    reasons = []
    for item in payload.get("alerts", []):
        if str(item.get("ticker", "")).upper() == ticker:
            reasons.append(f"earnings within {item.get('days_until')} day(s)")
    return reasons


def _portfolio_risk_reasons():
    payload = _load_json(PORTFOLIO_STATE_FILE, {})
    reasons = []
    why = payload.get("why_now", {})
    if why.get("send_alert"):
        reasons.append(f"portfolio risk trigger: {why.get('reason')}")
    drift = payload.get("drift_monitor", {})
    if drift.get("alerts"):
        reasons.append(f"{len(drift.get('alerts'))} portfolio positions outside drift band")
    return reasons


def scan(
    yf_module,
    tickers: Iterable[str],
    price_move_pct: float = 2.0,
    volume_ratio: float = 2.0,
    breakout_lookback: int = 20,
    interval: str = "5m",
    period: str = "5d",
) -> Dict:
    state = _load_state()
    alerts = []
    new_state = {}
    tickers = [t.upper() for t in tickers]

    for ticker in tickers:
        try:
            raw = yf_module.download(ticker, period=period, interval=interval, progress=False, auto_adjust=False)
            df = _normalize_download(raw)
            if df.empty:
                continue
            close, move, vol_ratio, z_score, abnormality, reasons = _ticker_reasons(df, price_move_pct, volume_ratio, breakout_lookback)
            reasons.extend(_earnings_alerts_for_ticker(ticker))
            alert_key = f"{ticker}:{df.index[-1]}:{'|'.join(sorted(set(reasons)))}"
            if reasons and state.get(ticker, {}).get("last_alert_key") != alert_key:
                alerts.append({"ticker": ticker, "price": close, "move_pct": move, "volume_ratio": vol_ratio, "z_score": z_score, "abnormality": abnormality, "reasons": sorted(set(reasons))})
                new_state[ticker] = {"last_alert_key": alert_key, "last_price": close, "updated_at": datetime.now().isoformat()}
            else:
                new_state[ticker] = {**state.get(ticker, {}), "last_price": close, "updated_at": datetime.now().isoformat()}
        except Exception as exc:
            new_state[ticker] = {"error": str(exc), "updated_at": datetime.now().isoformat()}

    portfolio_reasons = _portfolio_risk_reasons()
    if portfolio_reasons:
        key = f"portfolio:{'|'.join(portfolio_reasons)}"
        if state.get("_portfolio", {}).get("last_alert_key") != key:
            alerts.append({"ticker": "PORTFOLIO", "price": None, "move_pct": None, "volume_ratio": None, "reasons": portfolio_reasons})
            new_state["_portfolio"] = {"last_alert_key": key, "updated_at": datetime.now().isoformat()}

    _save_state(new_state)
    return {"generated_at": datetime.now().isoformat(), "interval": interval, "alerts": alerts}


def format_telegram(payload: Dict) -> str:
    if not payload.get("alerts"):
        return ""
    lines = ["TRIGGER SCANNER ALERT", ""]
    for item in payload.get("alerts", []):
        if item.get("ticker") == "PORTFOLIO":
            lines.append(f"PORTFOLIO: {', '.join(item['reasons'])}")
        else:
            z_text = f" | Z {item.get('z_score'):+.2f}" if item.get("z_score") is not None else ""
            lines.append(f"{item['ticker']}: ${item['price']:.2f}{z_text} | {', '.join(item['reasons'])}")
    lines.append("\nWhy it matters: a fast trigger changed before the next full 30-minute research cycle.")
    lines.append("Educational research alert only, not financial advice.")
    return "\n".join(lines)
