import requests
import json
import os

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
    tier = "Tier 5: Avoid"
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
    if alerts and "Initial data recorded" not in alerts[0]:
        report += "🎯 *Standard Alerts:*\n"
        for alert in alerts:
            report += f"  • {alert}\n"
    elif not event_list:
        report += "🎯 *Events:* No new technical changes.\n"
    
    # Section 3: Technical Snapshot
    trend = "Above SMA200" if latest['Close'] > latest['SMA200'] else "Below SMA200"
    rs_status = "Outperforming" if latest.get('RS_Line', 0) > latest.get('RS_SMA20', 0) else "Lagging"
    
    report += (
        f"📊 *Snapshot:*\n"
        f"  • Price: ${latest['Close']:.2f} ({trend})\n"
        f"  • RSI (W/M): {latest.get('RSI_Weekly', 0):.0f} / {latest.get('RSI_Monthly', 0):.0f}\n"
        f"  • RS vs SPY: {rs_status}\n"
    )
    return report + "------------------------------------------\n"

def send_bundle(full_report_list):
    """
    Groups all ticker reports into a single message.
    ONLY sends if a real event (New Events OR Standard Alerts) is present.
    """
    has_real_events = any(
        ("🎯 *Standard Alerts:*" in report) or ("🌟 *Critical Events:*" in report)
        for report in full_report_list
    )

    if not has_real_events:
        print("No technical events detected. Skipping Telegram.")
        return

    tg_config = load_telegram_config()
    token = os.getenv('TELEGRAM_BOT_TOKEN') or tg_config.get("token")
    chat_id = os.getenv('TELEGRAM_CHAT_ID') or tg_config.get("chat_id")
    
    if not token or not chat_id:
        print("Telegram Error: Missing credentials.")
        return

    # Construct Final Message
    message = "🔔 *JFO Technical Analytics Summary*\n"
    message += "============================\n\n"
    
    for ticker_report in full_report_list:
        if "No new technical changes" not in ticker_report:
            message += ticker_report

    message += "\n_Status: Analysis Complete_"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        response = requests.post(url, data=payload, timeout=10)
        if response.status_code == 200:
            print(f"Telegram report sent.")
        else:
            print(f"Telegram Failed: {response.text}")
    except Exception as e:
        print(f"Connection Error: {e}")
