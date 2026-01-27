import requests
import json
import os
import time
import re


# ============================================================
# CONFIG
# ============================================================

def load_telegram_config():
    config_path = os.path.join('config', 'config.json')
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config.get("telegram", {})
    except Exception:
        return {}


# ============================================================
# HELPERS
# ============================================================

def _safe_float(value):
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value.replace("%", "").replace("x", ""))
    except Exception:
        return None
    return None


def _escape_md(text: str) -> str:
    """Escape Telegram Markdown special characters."""
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', str(text))


# ============================================================
# REPORT FORMATTER
# ============================================================

def format_ticker_report(ticker, alerts, latest, rating_data):
    score = rating_data.get('score', 0)
    tier = rating_data.get('rating', 'N/A')
    metrics = rating_data.get('metrics', {})
    is_extended = rating_data.get('is_extended', False)

    tv_link = f"https://www.tradingview.com/symbols/{ticker}/"

    report = (
        f"🔍 *[{_escape_md(ticker)}]({_escape_md(tv_link)})* | "
        f"Score: `{score}/100`\n"
    )
    report += f"🏷 *Rank: {_escape_md(tier)}*\n"

    if is_extended:
        report += "⚠️ *STRETCHED: High Volatility Risk*\n"

    # ----------------------------
    # Critical Events
    # ----------------------------
    event_list = []

    if latest.get('Golden_Cross'):
        event_list.append("🚀 *GOLDEN CROSS*")

    rv = _safe_float(latest.get('RV'))
    if rv is not None and rv >= 2.0:
        event_list.append("📊 *INSTITUTIONAL VOLUME*")

    if latest.get('RS_Breakout'):
        event_list.append("⚡ *RS BREAKOUT*")

    if event_list:
        report += "🌟 *Critical Events:*\n"
        for event in event_list:
            report += f"  • {event}\n"

    # ----------------------------
    # Standard Alerts
    # ----------------------------
    if alerts and "Initial data recorded" not in str(alerts):
        report += "🎯 *Standard Alerts:*\n"
        for alert in alerts:
            report += f"  • {_escape_md(alert)}\n"
    elif not event_list:
        report += "🎯 *Events:* No new technical changes.\n"

    # ----------------------------
    # Snapshot
    # ----------------------------
    close_price = _safe_float(latest.get('Close'))
    sma200 = _safe_float(latest.get('SMA200'))
    weekly_rsi = _safe_float(metrics.get('weekly_rsi'))
    rsi_monthly = _safe_float(latest.get('RSI_Monthly'))
    mrs_value = _safe_float(metrics.get('mrs_value'))

    trend = "Unknown"
    if close_price is not None and sma200 is not None:
        trend = "Above SMA200" if close_price > sma200 else "Below SMA200"

    report += "📊 *Snapshot:*\n"

    report += (
        f"  • Price: ${close_price:.2f} ({trend})\n"
        if close_price is not None
        else "  • Price: N/A\n"
    )

    report += f"  • Rel. Volume: {metrics.get('rel_volume', 'N/A')}\n"

    report += (
        f"  • RS vs SPY: {mrs_value:+.1f} (MRS)\n"
        if mrs_value is not None
        else "  • RS vs SPY: N/A\n"
    )

    rsi_w = f"{weekly_rsi:.0f}" if weekly_rsi is not None else "N/A"
    rsi_m = f"{rsi_monthly:.0f}" if rsi_monthly is not None else "N/A"

    report += f"  • RSI (W/M): {rsi_w} / {rsi_m}\n"

    return report + "------------------------------------------\n"


# ============================================================
# TELEGRAM SENDING
# ============================================================

def send_long_message(message_text):
    tg_config = load_telegram_config()
    token = os.getenv('TELEGRAM_BOT_TOKEN') or tg_config.get("token")
    chat_id = os.getenv('TELEGRAM_CHAT_ID') or tg_config.get("chat_id")

    if not token or not chat_id:
        print("Telegram Error: Missing credentials.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    MAX_LENGTH = 4000

    while message_text:
        split_at = message_text.rfind('\n', 0, MAX_LENGTH)
        if split_at == -1:
            split_at = MAX_LENGTH

        chunk = message_text[:split_at]
        _execute_send(url, chat_id, chunk)
        message_text = message_text[split_at:].lstrip()
        time.sleep(0.7)


def _execute_send(url, chat_id, text, retries=3):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, data=payload, timeout=15)
            if response.status_code == 200:
                return
            time.sleep(1.5)
        except Exception:
            time.sleep(1.5)

    print("Telegram send failed after retries.")


# ============================================================
# BUNDLE DISPATCH
# ============================================================

def send_bundle(full_report_list, regime_label="Unknown"):
    if not full_report_list:
        return

    placeholders = [
        "_No active data._",
        "_No Tier 1 Leaders found today._",
        "_No significant drops found today._"
    ]

    significant = []
    for segment in full_report_list:
        if any(p in segment for p in placeholders):
            continue
        if "No new technical changes" in segment:
            continue
        significant.append(segment)

    if not significant:
        print("No actionable signals. Telegram message skipped.")
        return

    message = (
        "🏦 *JAIN FAMILY OFFICE: DAILY INTEL*\n"
        f"Regime: {_escape_md(regime_label)}\n"
        "============================\n\n"
    )

    for report in significant:
        message += report

    message += "\n_Status: Analysis Complete_"

    send_long_message(message)
