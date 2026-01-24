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

def format_ticker_report(ticker, alerts, latest, rating_data):
    """
    Creates a bundled, executive-friendly report for a single ticker.
    Includes TradingView links, Tier Ratings, and Volatility Guards.
    """
    score = rating_data['score']
    tier = rating_data['rating']
    metrics = rating_data['metrics']
    is_extended = rating_data.get('is_extended', False)
    
    # Generate TradingView Link for instant chart access
    tv_link = f"https://www.tradingview.com/symbols/{ticker}/"

    # Header: Ticker is now a clickable link
    report = f"🔍 *[{ticker}]({tv_link})* | Score: `{score}/100`\n"
    report += f"🏷 *Rank: {tier}*\n"
    
    # Volatility Warning
    if is_extended:
        report += "⚠️ *STRETCHED: High Volatility Risk*\n"
    
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
    
    # Section 3: Technical Snapshot (Multi-Timeframe & RS)
    sma200 = latest.get('SMA200', 0)
    trend = "Above SMA200" if latest['Close'] > sma200 else "Below SMA200"
    
    report += (
        f"📊 *Snapshot:*\n"
        f"  • Price: ${latest['Close']:.2f} ({trend})\n"
        f"  • Rel. Volume: {metrics.get('rel_volume', '1.0x')}\n"
        f"  • RS vs SPY: {metrics.get('mrs_value', 0):+.1f} (MRS)\n"
        f"  • RSI (W/M): {metrics.get('weekly_rsi', 'N/A')} / {latest.get('RSI_Monthly', 0):.0f}\n"
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
                time.sleep(0.6) # Avoid Telegram's flood/rate limit protection
            else:
                _execute_send(url, chat_id, message_text)
                break

def _execute_send(url, chat_id, text):
    """Performs the actual POST request to the Telegram API."""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True # Keeps the chat clean from 50+ link previews
    }
    try:
        response = requests.post(url, data=payload, timeout=15)
        if response.status_code != 200:
            print(f"Telegram API Error: {response.text}")
    except Exception as e:
        print(f"Connection Exception: {e}")

def send_bundle(full_report_list, regime_label="Unknown"):
    """
    Groups multiple ticker reports into a single automated dispatch.
    Includes Market Regime headers.
    """
    if not full_report_list:
        return

    # Construct the final master message
    message = f"🏦 **JAIN FAMILY OFFICE: DAILY INTEL**\n"
    message += f"Regime: {regime_label}\n"
    message += "============================\n\n"
    
    significant_reports = [r for r in full_report_list if "No new technical changes" not in r]
    
    if not significant_reports:
        print("No significant technical events to report.")
        return

    for ticker_report in significant_reports:
        message += ticker_report

    message += "\n_Status: Analysis Complete_"
    
    # Hand off to the chunking logic for delivery
    send_long_message(message)
