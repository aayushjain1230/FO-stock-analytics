import requests
import json
import os
import time


def load_telegram_config():
    """Reads credentials from the config file."""
    config_path = os.path.join('config', 'config.json')
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config.get("telegram", {})
    except Exception:
        return {}


def _safe_float(value, default=None):
    """Safely cast to float or return default."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def format_ticker_report(ticker, alerts, latest, rating_data):
    """
    Creates a bundled, executive-friendly report for a single ticker.
    Includes TradingView links, Tier Ratings, and Volatility Guards.
    """
    score = rating_data.get('score', 0)
    tier = rating_data.get('rating', 'N/A')
    metrics = rating_data.get('metrics', {})
    is_extended = rating_data.get('is_extended', False)

    # Generate TradingView Link
    tv_link = f"https://www.tradingview.com/symbols/{ticker}/"

    report = f"🔍 *[{ticker}]({tv_link})* | Score: `{score}/100`\n"
    report += f"🏷 *Rank: {tier}*\n"

    if is_extended:
        report += "⚠️ *STRETCHED: High Volatility Risk*\n"

    # Section 1: Critical Events
    event_list = []
    if latest.get('Golden_Cross'):
        event_list.append("🚀 *GOLDEN CROSS*")
    if latest.get('Volume_Spike'):
        event_list.append("📊 *INSTITUTIONAL VOLUME*")
    if latest.get('RS_Breakout'):
        event_list.append("⚡ *RS BREAKOUT*")

    if event_list:
        report += "🌟 *Critical Events:*\n"
        for event in event_list:
            report += f"  • {event}\n"

    # Section 2: Standard Alerts
    if alerts and "Initial data recorded" not in str(alerts):
        report += "🎯 *Standard Alerts:*\n"
        for alert in alerts:
            report += f"  • {alert}\n"
    elif not event_list:
        report += "🎯 *Events:* No new technical changes.\n"

    # -------- SAFE SNAPSHOT VALUES --------
    close_price = _safe_float(latest.get('Close'))
    sma200 = _safe_float(latest.get('SMA200'))
    rsi_monthly = _safe_float(latest.get('RSI_Monthly'))
    weekly_rsi = _safe_float(metrics.get('weekly_rsi'))
    mrs_value = _safe_float(metrics.get('mrs_value'))

    trend = "Unknown"
    if close_price is not None and sma200 is not None:
        trend = "Above SMA200" if close_price > sma200 else "Below SMA200"

    # Section 3: Technical Snapshot
    report += "📊 *Snapshot:*\n"

    if close_price is not None:
        report += f"  • Price: ${close_price:.2f} ({trend})\n"
    else:
        report += "  • Price: N/A\n"

    report += f"  • Rel. Volume: {metrics.get('rel_volume', 'N/A')}\n"

    if mrs_value is not None:
        report += f"  • RS vs SPY: {mrs_value:+.1f} (MRS)\n"
    else:
        report += "  • RS vs SPY: N/A\n"

    rsi_w = f"{weekly_rsi:.0f}" if weekly_rsi is not None else "N/A"
    rsi_m = f"{rsi_monthly:.0f}" if rsi_monthly is not None else "N/A"

    report += f"  • RSI (W/M): {rsi_w} / {rsi_m}\n"

    return report + "------------------------------------------\n"


def send_long_message(message_text):
    """
    Splits the final report into chunks to respect Telegram's 4096 char limit.
    """
    tg_config = load_telegram_config()
    token = os.getenv('TELEGRAM_BOT_TOKEN') or tg_config.get("token")
    chat_id = os.getenv('TELEGRAM_CHAT_ID') or tg_config.get("chat_id")

    if not token or not chat_id:
        print("Telegram Error: Missing credentials (Token or Chat ID).")
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
        time.sleep(0.6)


def _execute_send(url, chat_id, text):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        response = requests.post(url, data=payload, timeout=15)
        if response.status_code != 200:
            print(f"Telegram API Error: {response.text}")
    except Exception as e:
        print(f"Connection Exception: {e}")


def send_bundle(full_report_list, regime_label="Unknown"):
    """
    Combines multiple ticker reports into a single dispatch.
    Skips placeholder-only reports.
    """
    if not full_report_list:
        return

    placeholder_texts = [
        "_No active data._",
        "_No Tier 1 Leaders found today._",
        "_No significant drops found today._"
    ]

    significant_reports = []
    for segment in full_report_list:
        if any(p in segment for p in placeholder_texts):
            continue

        has_actual_alerts = any(marker in segment for marker in [
            "🌟 *Critical Events:*",
            "🎯 *Standard Alerts:*",
            "🚀 ENTERED STAGE 2",
            "⚡ RS BREAKOUT",
            "📊 VOLUME SPIKE",
            "🚀 Crossed ABOVE",
            "🔴 Crossed BELOW",
            "📈 Weekly RSI reclaimed",
            "🔥 BLUE SKY"
        ])

        if "No new technical changes" in segment and not has_actual_alerts:
            continue

        significant_reports.append(segment)

    if not significant_reports:
        print("No new technical events or alerts to report. Skipping Telegram notification.")
        return

    message = "🏦 **JAIN FAMILY OFFICE: DAILY INTEL**\n"
    message += f"Regime: {regime_label}\n"
    message += "============================\n\n"

    for report in significant_reports:
        message += report

    message += "\n_Status: Analysis Complete_"

    send_long_message(message)
