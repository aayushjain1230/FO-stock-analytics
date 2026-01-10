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

def format_ticker_report(ticker, alerts, latest):
    """
    Creates a bundled, executive-friendly report for a single ticker.
    Includes major technical events like Golden Crosses and Volume Spikes.
    """
    # Header
    report = f"🔍 *Ticker: {ticker}*\n"
    
    # Section 1: Major Technical Events (The "Big Hits")
    # These triggers are passed from the new scoring.py 'events' dictionary
    event_list = []
    if latest.get('Golden_Cross'): event_list.append("🚀 *GOLDEN CROSS DETECTED*")
    if latest.get('Volume_Spike'): event_list.append("📊 *INSTITUTIONAL VOLUME SPIKE*")
    if latest.get('RS_Breakout'): event_list.append("⚡ *RELATIVE STRENGTH BREAKOUT*")

    if event_list:
        report += "🌟 *Critical Events:*\n"
        for event in event_list:
            report += f"  • {event}\n"

    # Section 2: Standard Alerts (Price/SMA crossovers from state_manager)
    if alerts and "Initial data recorded" not in alerts[0]:
        report += "🎯 *Standard Alerts:*\n"
        for alert in alerts:
            report += f"  • {alert}\n"
    elif not event_list:
        report += "🎯 *Events:* No new technical changes.\n"
    
    # Section 3: Technical Snapshot (The "State")
    trend = "Above SMA200" if latest['Close'] > latest['SMA200'] else "Below SMA200"
    rs_status = "Outperforming" if latest.get('RS_Line', 0) > latest.get('RS_SMA20', 0) else "Lagging"
    
    report += (
        f"📊 *Snapshot:*\n"
        f"  • Price: ${latest['Close']:.2f} ({trend})\n"
        f"  • RSI (W/M): {latest['RSI_Weekly']:.0f} / {latest['RSI_Monthly']:.0f}\n"
        f"  • RS vs SPY: {rs_status}\n"
    )
    return report + "------------------------------------------\n"

def send_bundle(full_report_list):
    """
    Groups all ticker reports into a single message.
    ONLY sends if at least one ticker has a real technical event or alert.
    """
    # Check if any report contains a real event (New Events OR Standard Alerts)
    has_real_events = any(
        ("🎯 *Standard Alerts:*" in report) or ("🌟 *Critical Events:*" in report)
        for report in full_report_list
    )

    if not has_real_events:
        print("No technical events detected across all tickers. Skipping Telegram notification.")
        return

    tg_config = load_telegram_config()
    token = os.getenv('TELEGRAM_BOT_TOKEN') or tg_config.get("token")
    chat_id = os.getenv('TELEGRAM_CHAT_ID') or tg_config.get("chat_id")
    
    is_enabled = tg_config.get("enabled", False) or (token and chat_id)

    if not is_enabled:
        print("Telegram notifications are disabled.")
        return

    if not token or not chat_id:
        print("Telegram Error: Missing token or chat_id.")
        return

    # Construct the final message
    message = "🔔 *JFO Technical Analytics Summary*\n"
    message += "============================\n\n"
    
    for ticker_report in full_report_list:
        # Only include tickers that actually have something to report to keep it clean
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
            print(f"Telegram report sent successfully.")
        else:
            print(f"Telegram Failed: {response.text}")
    except Exception as e:
        print(f"Connection Error: {e}")
