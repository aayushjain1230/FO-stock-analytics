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

def format_ticker_report(ticker, alerts, latest, score=0):
    """
    Creates a bundled, executive-friendly report for a single ticker.
    Includes Tier Rating, Score, and Critical Events.
    """
    # Header & Rating Logic
    tier = "Tier 5: Avoid 🔴"
    if score >= 80: tier = "Tier 1: Market Leader 🏆"
    elif score >= 60: tier = "Tier 2: Improving 📈"
    elif score >= 40: tier = "Tier 3: Neutral ⚖️"
    elif score >= 20: tier = "Tier 4: Lagging 📉"

    report = f"🔍 *Ticker: {ticker}* | `{score}/100`\n"
    report += f"🏷 *Rating: {tier}*\n"
    
    # Section 1: Major Technical Events (The "Big Hits")
    event_list = []
    if latest.get('Golden_Cross'): event_list.append("🚀 *GOLDEN CROSS*")
    if latest.get('Volume_Spike'): event_list.append("📊 *INSTITUTIONAL VOLUME*")
    if latest.get('RS_Breakout'): event_list.append("⚡ *RS BREAKOUT*")

    if event_list:
        report += "🌟 *Critical Events:*\n"
        for event in event_list:
            report += f"  • {event}\n"

    # Section 2: Standard Alerts (State Transitions)
    if alerts and "Initial data recorded" not in str(alerts):
        report += "🎯 *Standard Alerts:*\n"
        for alert in alerts:
            report += f"  • {alert}\n"
    elif not event_list:
        report += "🎯 *Events:* No new technical changes.\n"
    
    # Section 3: Technical Snapshot (Enhanced with MRS and RV)
    sma200 = latest.get('SMA200', 0)
    trend = "Above SMA200" if latest['Close'] > sma200 else "Below SMA200"
    
    rv = latest.get('RV', 1.0)
    mrs = latest.get('MRS', 0)
    mrs_status = "💪 Strong" if mrs > 0 else "😴 Weak"
    
    report += (
        f"📊 *Snapshot:*\n"
        f"  • Price: ${latest['Close']:.2f} ({trend})\n"
        f"  • Rel. Volume: {rv:.2f}x (Avg: 1.0x)\n"
        f"  • RSI (W/M): {latest.get('RSI_Weekly', 0):.0f} / {latest.get('RSI_Monthly', 0):.0f}\n"
        f"  • RS vs SPY: {mrs_status} ({mrs:+.1f})\n"
    )
    return report + "------------------------------------------\n"

def send_long_message(message_text):
    """
    Splits the final report into chunks to respect Telegram's 4096 char limit.
    Ensures that reports for individual tickers are not cut in half.
    """
    tg_config = load_telegram_config()
    token = os.getenv('TELEGRAM_BOT_TOKEN') or tg_config.get("token")
    chat_id = os.getenv('TELEGRAM_CHAT_ID') or tg_config.get("chat_id")
    
    if not token or not chat_id:
        print("Telegram Error: Missing credentials (Token or Chat ID).")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    MAX_LENGTH = 4000 # Safe buffer below the 4096 limit

    if len(message_text) <= MAX_LENGTH:
        _execute_send(url, chat_id, message_text)
    else:
        while len(message_text) > 0:
            if len(message_text) > MAX_LENGTH:
                # Search for the last newline within the limit to split cleanly
                split_at = message_text.rfind('\n', 0, MAX_LENGTH)
                if split_at == -1: split_at = MAX_LENGTH
                
                chunk = message_text[:split_at]
                _execute_send(url, chat_id, chunk)
                
                # Remove the sent chunk and leading whitespace
                message_text = message_text[split_at:].lstrip()
                time.sleep(0.5) # Avoid Telegram's flood/rate limit protection
            else:
                _execute_send(url, chat_id, message_text)
                break

def _execute_send(url, chat_id, text):
    """Performs the actual POST request to the Telegram API."""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, data=payload, timeout=15)
        if response.status_code != 200:
            print(f"Telegram API Error: {response.text}")
    except Exception as e:
        print(f"Connection Exception: {e}")

def send_bundle(full_report_list):
    """
    Groups multiple ticker reports into a single automated dispatch.
    Only triggers if a 'Critical Event' or 'Standard Alert' is detected.
    """
    has_real_events = any(
        ("🎯 *Standard Alerts:*" in report) or ("🌟 *Critical Events:*" in report)
        for report in full_report_list
    )

    if not has_real_events:
        print("No significant technical events to report. Skipping dispatch.")
        return

    # Construct the final master message
    message = "🔔 *JFO Technical Analytics Summary*\n"
    message += "============================\n\n"
    
    for ticker_report in full_report_list:
        # Exclude tickers that are just 'snapshot only' with no new changes
        if "No new technical changes" not in ticker_report:
            message += ticker_report

    message += "\n_Status: Daily Analysis Complete_"
    
    # Hand off to the chunking logic for delivery
    send_long_message(message)
