import requests
import json
import os
import time
import re

# ============================================================
# CONFIGURATION LOADER
# ============================================================

def load_telegram_config():
    """Load credentials from config/config.json or environment variables."""
    config_path = os.path.join('config', 'config.json')
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = json.load(f)
                return config.get("telegram", {})
    except Exception as e:
        print(f"Config Load Error: {e}")
    return {}

# ============================================================
# ROBUST HELPERS (MarkdownV2 & Math)
# ============================================================

def _safe_float(value):
    """Safely convert strings like '10.5%' or '2.0x' to float."""
    try:
        if value is None: return None
        if isinstance(value, (int, float)): return float(value)
        if isinstance(value, str):
            return float(value.replace("%", "").replace("x", "").replace(",", ""))
    except:
        return None
    return None

def _escape_md(text: str) -> str:
    """
    STRICT MarkdownV2 Escaping. 
    Telegram V2 will FAIL to send if even a single '.' or '-' is unescaped.
    """
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', str(text))

# ============================================================
# REPORT FORMATTERS
# ============================================================

def format_ticker_report(ticker, alerts, latest, rating_data):
    """
    Watchlist Detailer. 
    Shows Score, Rank, Events, and the Full Snapshot block.
    """
    score = rating_data.get('score', 0)
    tier = rating_data.get('rating', 'N/A')
    metrics = rating_data.get('metrics', {})
    is_extended = rating_data.get('is_extended', False)

    tv_link = f"https://www.tradingview.com/symbols/{ticker}/"

    # Header with Hyperlink
    report = f"⭐ *[{_escape_md(ticker)}]({_escape_md(tv_link)})* | Score: `{score}/100`\n"
    report += f"🏷 *Rank: {_escape_md(tier)}*\n"

    if is_extended:
        report += "⚠️ *STRETCHED: High Volatility Risk*\n"

    # 1. Critical Events
    event_list = []
    if latest.get('Golden_Cross'): event_list.append("🚀 *GOLDEN CROSS*")
    
    rv = _safe_float(latest.get('RV'))
    if rv and rv >= 2.0: 
        event_list.append(f"📊 *VOLUME SPIKE ({rv:.1f}x)*")
        
    if latest.get('RS_Breakout'): 
        event_list.append("⚡ *RS BREAKOUT*")

    if event_list:
        report += "🌟 *Technical Events:*\n"
        for event in event_list:
            report += f"  • {event}\n"

    # 2. Alerts (Only significant ones)
    if alerts and "Initial data recorded" not in str(alerts):
        report += "🎯 *Specific Alerts:*\n"
        for alert in alerts:
            report += f"  • {_escape_md(alert)}\n"
    elif not event_list:
        report += "🎯 *Events:* No new technical shifts.\n"

    # 3. The Full Snapshot Block
    price = _safe_float(latest.get('Close'))
    sma200 = _safe_float(latest.get('SMA200'))
    mrs = _safe_float(metrics.get('mrs_value'))
    rsi_w = _safe_float(metrics.get('weekly_rsi'))
    rsi_m = _safe_float(latest.get('RSI_Monthly'))
    d_high = metrics.get('dist_52w_high', 'N/A')
    d_low = metrics.get('dist_52w_low', 'N/A')

    trend = "Above SMA200" if (price and sma200 and price > sma200) else "Below SMA200"

    report += "📊 *Snapshot:*\n"
    if price:
        report += f"  • Price: ${price:.2f} ({trend})\n"
    
    report += f"  • Rel. Volume: {metrics.get('rel_volume', 'N/A')}\n"
    
    if mrs is not None:
        report += f"  • RS vs SPY: {mrs:+.1f} (MRS)\n"
    
    rw_str = f"{rsi_w:.0f}" if rsi_w else "N/A"
    rm_str = f"{rsi_m:.0f}" if rsi_m else "N/A"
    report += f"  • RSI (W/M): {rw_str} / {rm_str}\n"
    
    # 52-Week Bounds
    report += f"  • 52W Range: 🔽 {d_low} from low | 🔼 {d_high} to high\n"

    return report + "------------------------------------------\n"

def format_sector_summary(sector_map):
    """
    The 11-Sector Organizer.
    Identifies Highs and Lows for each group.
    """
    if not sector_map:
        return ""

    report = "📂 *SECTOR PERFORMANCE (S&P 500)*\n"
    report += "------------------------------------------\n"

    # Sort sectors by change %
    sorted_names = sorted(sector_map.keys(), 
                          key=lambda x: _safe_float(sector_map[x].get('change', 0)) or 0, 
                          reverse=True)

    for name in sorted_names:
        data = sector_map[name]
        chg = _safe_float(data.get('change', 0.0)) or 0.0
        top = data.get('top', 'N/A')
        bottom = data.get('bottom', 'N/A')
        
        emoji = "🟢" if chg >= 0 else "🔴"
        report += f"{emoji} *{_escape_md(name)}* ({chg:+.2f}%)\n"
        report += f"    🔼 Leader: `{top}` | 🔽 Laggard: `{bottom}`\n"

    return report

# ============================================================
# TELEGRAM DISPATCH ENGINE
# ============================================================

def _execute_send(url, chat_id, text, retries=3):
    """Handles the actual POST request with retry logic."""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, data=payload, timeout=20)
            if response.status_code == 200:
                return True
            print(f"Attempt {attempt+1} failed: {response.text}")
            time.sleep(2)
        except Exception as e:
            print(f"Network Error: {e}")
            time.sleep(2)
    return False

def send_long_message(message_text):
    """Chunks long text into 4000-char blocks to respect API limits."""
    config = load_telegram_config()
    token = os.getenv('TELEGRAM_BOT_TOKEN') or config.get("token")
    chat_id = os.getenv('TELEGRAM_CHAT_ID') or config.get("chat_id")

    if not token or not chat_id:
        print("Telegram Failure: Credentials missing.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    MAX_LENGTH = 4000

    while message_text:
        if len(message_text) <= MAX_LENGTH:
            _execute_send(url, chat_id, message_text)
            break
        
        split_at = message_text.rfind('\n', 0, MAX_LENGTH)
        if split_at == -1: split_at = MAX_LENGTH

        chunk = message_text[:split_at]
        _execute_send(url, chat_id, chunk)
        
        message_text = message_text[split_at:].lstrip()
        time.sleep(0.8)

# ============================================================
# BUNDLE COORDINATOR
# ============================================================

def send_bundle(watchlist_reports, sector_map, regime_label="Unknown"):
    """
    Compiles everything into a single, high-intel daily intel report.
    """
    if not watchlist_reports and not sector_map:
        print("No significant activity detected. Quiet mode enabled.")
        return

    # Header
    message = (
        "🏦 *JAIN FAMILY OFFICE: DAILY INTEL*\n"
        f"Market Regime: {_escape_md(regime_label)}\n"
        "============================\n\n"
    )

    # Section 1: Watchlist
    if watchlist_reports:
        message += "⭐ *WATCHLIST UPDATE*\n"
        for r in watchlist_reports:
            message += r
        message += "\n"

    # Section 2: 11 Sectors
    if sector_map:
        message += format_sector_summary(sector_map)

    # Footer
    message += f"\n_Status: Analysis Complete ({time.strftime('%Y-%m-%d')})_"

    # Send it
    send_long_message(message)
