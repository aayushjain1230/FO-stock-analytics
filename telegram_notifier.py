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
    if text is None: return ""
    # Reserved characters in Telegram MarkdownV2
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', str(text))

# ============================================================
# REPORT FORMATTERS
# ============================================================

def format_ticker_report(ticker, alerts, latest, rating_data):
    """
    Watchlist Detailer. 
    Shows Score, Rank, Events, and the Full Snapshot block with 3-run trend.
    """
    score = rating_data.get('score', 0)
    tier = rating_data.get('rating', 'N/A')
    metrics = rating_data.get('metrics', {})
    is_extended = rating_data.get('is_extended', False)

    # --- 3-RUN PRICE TREND LOGIC ---
    history = rating_data.get('price_history', [])
    trend_emoji = ""
    if len(history) >= 3:
        # Check last 3 data points
        p1, p2, p3 = history[-3], history[-2], history[-1]
        if p3 > p2 > p1:
            trend_emoji = " (🚀 🔼🔼🔼)"
        elif p3 < p2 < p1:
            trend_emoji = " (⚠️ 🔽🔽🔽)"
        else:
            trend_emoji = " (↔️)"

    tv_link = f"https://www.tradingview.com/symbols/{ticker}/"

    # Header with Hyperlink
    report = f"⭐ *[{_escape_md(ticker)}]({_escape_md(tv_link)})* | Score: `{_escape_md(str(score))}/100`\n"
    report += f"🏷 *Rank: {_escape_md(tier)}*\n"

    if is_extended:
        report += _escape_md("⚠️ STRETCHED: High Volatility Risk") + "\n"

    # 1. Critical Events
    event_list = []
    if latest.get('Golden_Cross'): event_list.append("🚀 *GOLDEN CROSS*")
    
    rv_val = _safe_float(latest.get('RV'))
    if rv_val and rv_val >= 2.0: 
        event_list.append(f"📊 *VOLUME SPIKE ({_escape_md(f'{rv_val:.1f}x')})*")
        
    if latest.get('RS_Breakout'): 
        event_list.append("⚡ *RS BREAKOUT*")

    if event_list:
        report += "🌟 *Technical Events:*\n"
        for event in event_list:
            report += f"  • {event}\n"

    # 2. Alerts
    if alerts and "Initial data recorded" not in str(alerts):
        report += "🎯 *Specific Alerts:*\n"
        for alert in alerts:
            report += f"  • {_escape_md(alert)}\n"
    elif not event_list:
        report += _escape_md("🎯 Events: No new technical shifts.") + "\n"

    # 3. The Full Snapshot Block
    price = _safe_float(latest.get('Close'))
    sma200 = _safe_float(latest.get('SMA200'))
    mrs = _safe_float(metrics.get('mrs_value'))
    rsi_w = _safe_float(metrics.get('weekly_rsi'))
    rsi_m = _safe_float(latest.get('RSI_Monthly'))
    d_high = metrics.get('dist_52w_high', 'N/A')
    d_low = metrics.get('dist_52w_low', 'N/A')

    trend_label = "Above SMA200" if (price and sma200 and price > sma200) else "Below SMA200"

    report += "📊 *Snapshot:*\n"
    if price:
        report += f"  • Price: ${_escape_md(f'{price:.2f}')}{_escape_md(trend_emoji)}\n"
    
    report += f"  • SMA200: {_escape_md(trend_label)}\n"
    report += f"  • Rel. Volume: {_escape_md(str(metrics.get('rel_volume', 'N/A')))}\n"
    
    if mrs is not None:
        report += f"  • RS vs SPY: {_escape_md(f'{mrs:+.1f}')} (MRS)\n"
    
    rw_str = f"{rsi_w:.0f}" if rsi_w else "N/A"
    rm_str = f"{rsi_m:.0f}" if rsi_m else "N/A"
    report += f"  • RSI (W/M): {_escape_md(rw_str)} / {_escape_md(rm_str)}\n"
    
    report += f"  • 52W Range: 🔽 {_escape_md(str(d_low))} from low | 🔼 {_escape_md(str(d_high))} to high\n"

    return report + _escape_md("------------------------------------------") + "\n"

def format_sector_summary(sector_map):
    """
    The 11-Sector Organizer.
    Professional multi-line formatting with tree-style layout and emojis.
    """
    if not sector_map:
        return ""

    sector_emojis = {
        "Information Technology": "💻", "Health Care": "🏥", "Financials": "🏦",
        "Consumer Discretionary": "🛍️", "Communication Services": "📡",
        "Industrials": "⚙️", "Consumer Staples": "🛒", "Energy": "🛢️",
        "Utilities": "⚡", "Real Estate": "🏢", "Materials": "🏗️"
    }

    report = _escape_md("📂 SECTOR PERFORMANCE (S&P 500)") + "\n"
    report += _escape_md("------------------------------------------") + "\n"

    # Sort sectors by change %
    sorted_names = sorted(sector_map.keys(), 
                          key=lambda x: _safe_float(sector_map[x].get('change', 0)) or 0, 
                          reverse=True)

    for name in sorted_names:
        data = sector_map[name]
        chg = _safe_float(data.get('change', 0.0)) or 0.0
        top = data.get('top', 'N/A')
        bottom = data.get('bottom', 'N/A')
        
        s_emoji = sector_emojis.get(name, "📁")
        perf_emoji = "🟢" if chg >= 0 else "🔴"
        
        # Professional Multi-line layout
        report += f"{perf_emoji} {s_emoji} *{_escape_md(name)}* ({_escape_md(f'{chg:+.2f}%')})\n"
        report += f"    ├ 🏆 Leader: `{_escape_md(top)}`\n"
        report += f"    └ 📉 Laggard: `{_escape_md(bottom)}`\n\n"

    return report

# ============================================================
# TELEGRAM DISPATCH ENGINE
# ============================================================

def _execute_send(url, chat_id, text, retries=3):
    """Handles the actual POST request with retry logic and parsing fallback."""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, json=payload, timeout=20)
            if response.status_code == 200:
                return True
            
            # If MarkdownV2 fails, fallback to plain text safety net
            if response.status_code == 400 and "can't parse entities" in response.text:
                print(f"Parsing failure on attempt {attempt+1}. Retrying as plain text...")
                payload["parse_mode"] = ""
                payload["text"] = text.replace("\\", "")
                response = requests.post(url, json=payload, timeout=20)
                if response.status_code == 200: return True

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
        _escape_md("🏦 JAIN FAMILY OFFICE: DAILY INTEL") + "\n"
        f"Market Regime: {_escape_md(regime_label)}\n"
        f"{_escape_md('============================')}\n\n"
    )

    # Section 1: Watchlist Update
    if watchlist_reports:
        message += "⭐ *WATCHLIST UPDATE*\n"
        for r in watchlist_reports:
            message += r
        message += "\n"

    # Section 2: 11 Sectors Overview
    if sector_map:
        message += format_sector_summary(sector_map)

    # Footer
    curr_date = time.strftime('%Y-%m-%d')
    message += f"\n_Status: Analysis Complete ({_escape_md(curr_date)})_"

    # Send it
    send_long_message(message)
