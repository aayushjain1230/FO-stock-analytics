import json
import os
import re
import time

import requests


def load_telegram_config():
    """Load Telegram settings from config/config.json."""
    config_path = os.path.join("config", "config.json")
    try:
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config = json.load(f)
                return config.get("telegram", {})
    except Exception as e:
        print(f"Config Load Error: {e}")
    return {}


def _safe_float(value):
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value.replace("%", "").replace("x", "").replace(",", ""))
    except Exception:
        return None
    return None


def _escape_md(text: str) -> str:
    if text is None:
        return ""
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", str(text))


def _format_metric_line(label, value):
    return f"*{_escape_md(label)}:* {_escape_md(value)}"


def _direction_label(daily_change):
    value = _safe_float(daily_change)
    if value is None:
        return "Flat"
    if value > 0:
        return f"Up {value:+.2f}%"
    if value < 0:
        return f"Down {value:+.2f}%"
    return "Flat 0.00%"


def format_ticker_report(ticker, alerts, latest, rating_data, daily_change=None):
    """Return one polished Telegram card per ticker."""
    metrics = rating_data.get("metrics", {})
    score = rating_data.get("score", 0)
    rating = rating_data.get("rating", "N/A")
    confidence = rating_data.get("confidence")
    risk_level = rating_data.get("risk_level", "N/A")
    why_now = rating_data.get("why_now", {}) or {}
    close_price = _safe_float(metrics.get("close", latest.get("Close")))
    weekly_rsi = metrics.get("weekly_rsi", "N/A")
    monthly_rsi = metrics.get("monthly_rsi", "N/A")
    sma200 = _safe_float(latest.get("SMA200"))
    mrs_value = _safe_float(metrics.get("mrs_value"))

    above_sma200 = "Above SMA200" if close_price is not None and sma200 is not None and close_price >= sma200 else "Below SMA200"
    rs_status = "Leading SPY" if mrs_value is not None and mrs_value > 0 else "Lagging SPY"
    clean_alerts = [alert for alert in (alerts or []) if "Initial data recorded" not in alert]

    message_lines = [
        f"*{_escape_md(ticker)}*",
        f"`{_escape_md(_direction_label(daily_change))}`   {_escape_md(f'Score {score}/100')}   {_escape_md(rating)}",
        "",
        "*Triggered events*",
    ]

    if why_now.get("send_alert"):
        message_lines.extend([
            "",
            "*Why now*",
            _format_metric_line("Reason", why_now.get("reason", "N/A")),
            _format_metric_line("Evidence", why_now.get("evidence", "N/A")),
            _format_metric_line("Invalidates", why_now.get("invalidates", "N/A")),
        ])

    if clean_alerts:
        message_lines.extend([f"- {_escape_md(alert)}" for alert in clean_alerts])
    else:
        message_lines.append("- Notable price move without a new state transition")

    message_lines.extend(
        [
            "",
            "*Snapshot metrics*",
            _format_metric_line("Close", f"${close_price:.2f}" if close_price is not None else "N/A"),
            _format_metric_line("Weekly RSI", weekly_rsi),
            _format_metric_line("Monthly RSI", monthly_rsi),
            _format_metric_line("SMA200", above_sma200),
            _format_metric_line("RS Status", rs_status),
            _format_metric_line("Risk", risk_level),
            _format_metric_line("Confidence", f"{confidence}/100" if confidence is not None else "N/A"),
        ]
    )

    return "\n".join(message_lines)


def format_run_header(run_summary):
    lines = [
        "*Jain Family Office | Market Intel*",
        _format_metric_line("Market regime", run_summary.get("market_regime", "Unknown")),
        _format_metric_line("Interesting tickers", str(run_summary.get("interesting_count", 0))),
        _format_metric_line("Watchlist scanned", str(run_summary.get("watchlist_count", 0))),
        _format_metric_line("Market health", f"{run_summary.get('market_health_score', 'N/A')}/100"),
        _format_metric_line("Risk environment", run_summary.get("risk_environment", "N/A")),
        _format_metric_line("Buy environment", run_summary.get("buy_environment", "N/A")),
    ]

    biggest_up = run_summary.get("biggest_up")
    if biggest_up:
        lines.append(_format_metric_line("Top mover", f"{biggest_up['ticker']} {biggest_up['daily_change']:+.2f}%"))

    biggest_down = run_summary.get("biggest_down")
    if biggest_down:
        lines.append(_format_metric_line("Weakest mover", f"{biggest_down['ticker']} {biggest_down['daily_change']:+.2f}%"))

    return "\n".join(lines)


def format_sector_summary(sector_map, regime_label="Unknown"):
    if not sector_map:
        return ""

    report = [
        "*Market Breadth*",
        _format_metric_line("Regime", regime_label),
        "",
        "*Sector performance*",
    ]

    sorted_names = sorted(
        sector_map.keys(),
        key=lambda name: _safe_float(sector_map[name].get("change", 0)) or 0,
        reverse=True,
    )

    for name in sorted_names[:6]:
        data = sector_map[name]
        change = _safe_float(data.get("change", 0.0)) or 0.0
        top = data.get("top", "N/A")
        bottom = data.get("bottom", "N/A")
        report.append(
            f"- {_escape_md(name)}: {_escape_md(f'{change:+.2f}%')} | Leader: `{_escape_md(top)}` | Laggard: `{_escape_md(bottom)}`"
        )

    report.append("")
    report.append(_format_metric_line("Status", f"Analysis complete ({time.strftime('%Y-%m-%d')})"))
    return "\n".join(report)


def _execute_send(url, chat_id, text, retries=3):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, json=payload, timeout=20)
            if response.status_code == 200:
                return True

            if response.status_code == 400 and "can't parse entities" in response.text:
                payload["parse_mode"] = ""
                payload["text"] = text.replace("\\", "")
                response = requests.post(url, json=payload, timeout=20)
                if response.status_code == 200:
                    return True

            print(f"Attempt {attempt + 1} failed: {response.text}")
            time.sleep(2)
        except Exception as e:
            print(f"Network Error: {e}")
            time.sleep(2)
    return False


def send_long_message(message_text):
    """Send a message, chunking only when needed for Telegram limits."""
    config = load_telegram_config()
    if not config.get("enabled", True):
        print("Telegram disabled by config.")
        return

    token = os.getenv("TELEGRAM_BOT_TOKEN") or config.get("token")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or config.get("chat_id")

    if not token or not chat_id:
        print("Telegram Failure: Credentials missing.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    max_length = 4000

    while message_text:
        if len(message_text) <= max_length:
            _execute_send(url, chat_id, message_text)
            break

        split_at = message_text.rfind("\n", 0, max_length)
        if split_at == -1:
            split_at = max_length

        chunk = message_text[:split_at]
        _execute_send(url, chat_id, chunk)
        message_text = message_text[split_at:].lstrip()
        time.sleep(0.8)


def send_bundle(watchlist_reports, sector_map, regime_label="Unknown", run_summary=None):
    """
    Send a compact run summary, then only the interesting ticker cards.
    """
    if not watchlist_reports:
        print("No significant activity detected. Quiet mode enabled.")
        return False

    run_summary = run_summary or {}
    interesting_count = len(watchlist_reports)
    include_header = interesting_count > 1
    include_sector_summary = bool(sector_map) and interesting_count > run_summary.get("compact_mode_max_tickers", 2)

    if include_header:
        send_long_message(format_run_header(run_summary))
        time.sleep(0.5)

    for report in watchlist_reports:
        send_long_message(report)
        time.sleep(0.5)

    if include_sector_summary:
        sector_summary = format_sector_summary(sector_map, regime_label=regime_label)
        if sector_summary:
            send_long_message(sector_summary)

    return True
