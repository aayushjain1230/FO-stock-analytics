import os
import json
import time
import requests
import pandas as pd
import pandas_ta as ta
import pandas_market_calendars as mcal
import pytz
import yfinance as yf
from datetime import datetime

# ==========================================
# 1. TELEGRAM & NOTIFICATION ENGINE
# ==========================================
def load_telegram_config():
    config_path = os.path.join('config', 'config.json')
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config.get("telegram", {})
    except: return {}

def format_ticker_report(ticker, alerts, latest, rating_data):
    score = rating_data['score']
    tier = rating_data['rating']
    metrics = rating_data['metrics']
    is_extended = rating_data.get('is_extended', False)
    tv_link = f"https://www.tradingview.com/symbols/{ticker}/"

    report = f"🔍 *[{ticker}]({tv_link})* | Score: `{score}/100`\n"
    report += f"🏷 *Rank: {tier}*\n"
    if is_extended: report += "⚠️ *STRETCHED: High Risk*\n"
    
    event_list = []
    if latest.get('Golden_Cross'): event_list.append("🚀 *GOLDEN CROSS*")
    if latest.get('Volume_Spike'): event_list.append("📊 *INSTITUTIONAL VOLUME*")
    if latest.get('RS_Breakout'): event_list.append("⚡ *RS BREAKOUT*")

    if event_list:
        report += "🌟 *Critical Events:*\n"
        for event in event_list: report += f"  • {event}\n"

    if alerts and "Initial" not in str(alerts):
        report += "🎯 *Alerts:* " + ", ".join(alerts) + "\n"
    
    report += (f"📊 RS: `{metrics.get('mrs_value', 0):+.1f}` | Vol: `{metrics.get('rel_volume', '1.0x')}`\n")
    return report + "--------------------------\n"

def send_long_message(message_text):
    tg_config = load_telegram_config()
    token = os.getenv('TELEGRAM_BOT_TOKEN') or tg_config.get("token")
    chat_id = os.getenv('TELEGRAM_CHAT_ID') or tg_config.get("chat_id")
    if not token or not chat_id: return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for i in range(0, len(message_text), 4000):
        chunk = message_text[i:i+4000]
        payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "Markdown", "disable_web_page_preview": True}
        requests.post(url, data=payload, timeout=15)
        time.sleep(0.6)

# ==========================================
# 2. INDICATORS & SCORING ENGINE
# ==========================================
def calculate_metrics(df, benchmark_df):
    df, benchmark_df = df.align(benchmark_df, join='inner', axis=0)
    df['SMA50'] = ta.sma(df['Close'], length=50)
    df['SMA200'] = ta.sma(df['Close'], length=200)
    df['SMA20'] = ta.sma(df['Close'], length=20)
    
    df['RV'] = df['Volume'] / ta.sma(df['Volume'], length=20)
    df['Volume_Spike'] = df['RV'] >= 2.0
    
    df['RS_Line'] = df['Close'] / benchmark_df['Close']
    df['RS_SMA50'] = ta.sma(df['RS_Line'], length=50)
    df['MRS'] = ((df['RS_Line'] / df['RS_SMA50']) - 1) * 100
    df['RS_SMA20'] = ta.sma(df['RS_Line'], length=20)
    
    df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
    df['Dist_SMA20'] = (df['Close'] - df['SMA20']) / df['ATR']
    
    df['RSI_Weekly'] = ta.rsi(df['Close'], length=14) # Simplified for main.py consolidation
    df['Golden_Cross'] = (df['SMA50'] > df['SMA200']) & (df['SMA50'].shift(1) <= df['SMA200'].shift(1))
    df['RS_Breakout'] = (df['MRS'] > 0) & (df['MRS'].shift(1) <= 0)
    return df

def generate_rating(df):
    latest = df.iloc[-1]
    score = 0
    is_stage_2 = latest['Close'] > latest['SMA50'] > latest['SMA200']
    if is_stage_2: score += 40
    if latest['MRS'] > 0: score += 20
    if latest.get('RV', 0) >= 2.0: score += 20
    if latest.get('RS_Breakout'): score += 10
    
    is_extended = latest.get('Dist_SMA20', 0) > 3.0
    if is_extended: score -= 25

    if score >= 80: rating = "Tier 1: Leader 🏆"
    elif score >= 60: rating = "Tier 2: Improving 📈"
    else: rating = "Tier 3: Avoid 🔴"

    return {
        "score": max(0, score), "rating": rating, "is_extended": is_extended,
        "metrics": {"mrs_value": round(latest['MRS'], 2), "rel_volume": f"{round(latest['RV'], 2)}x"}
    }

# ==========================================
# 3. STATE MANAGEMENT
# ==========================================
def load_state():
    path = os.path.join('state', 'state.json')
    if os.path.exists(path):
        with open(path, 'r') as f: return json.load(f)
    return {}

def save_state(state):
    os.makedirs('state', exist_ok=True)
    with open(os.path.join('state', 'state.json'), 'w') as f:
        json.dump(state, f, indent=4)

# ==========================================
# 4. MAIN EXECUTION ENGINE
# ==========================================
def get_sp500_sectors():
    try:
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        table['Symbol'] = table['Symbol'].str.replace('.', '-', regex=False)
        return dict(zip(table['Symbol'], table['GICS Sector']))
    except: return {}

def is_market_open():
    nyse = mcal.get_calendar('NYSE')
    now = datetime.now(pytz.utc)
    schedule = nyse.schedule(start_date=now, end_date=now)
    return not schedule.empty and (schedule.iloc[0].market_open <= now <= schedule.iloc[0].market_close)

def run_analytics_engine():
    print("--- JFO Engine Starting ---")
    if not is_market_open():
        print("Market closed. Skipping.")
        return

    config_path = os.path.join('config', 'config.json')
    with open(config_path, 'r') as f: config = json.load(f)
    
    watchlist = config.get("watchlist", [])
    sector_map = get_sp500_sectors()
    manual_input = os.getenv('MANUAL_TICKERS', '')
    
    full_scan_list = [t.strip().upper() for t in manual_input.split(',')] if manual_input else list(set(watchlist + list(sector_map.keys())))
    
    batch_data = yf.download(full_scan_list, period="1y", group_by='ticker', threads=True)
    benchmark_data = yf.download("SPY", period="1y")
    
    prev_state = load_state()
    current_state = prev_state.copy()
    watchlist_reports, sector_reports = [], {}

    for ticker in full_scan_list:
        try:
            stock_data = batch_data[ticker].dropna()
            if stock_data.empty: continue
            
            analyzed = calculate_metrics(stock_data, benchmark_data)
            rating = generate_rating(analyzed)
            
            # Simple Alerting Logic
            alerts = []
            if rating['score'] >= 80 and prev_state.get(ticker, {}).get('score', 0) < 80:
                alerts.append("🔥 NEW LEADER")

            report_line = format_ticker_report(ticker, alerts, analyzed.iloc[-1], rating)
            
            if ticker in watchlist:
                watchlist_reports.append(report_line)
            elif rating['score'] >= 80:
                sec = sector_map.get(ticker, "Other")
                sector_reports[sec] = sector_reports.get(sec, "") + report_line
            
            current_state[ticker] = {"score": rating['score'], "price": float(analyzed.iloc[-1]['Close'])}
        except: continue

    # Build Final Message
    final_msg = "🚀 **JFO MARKET INTEL**\n\n📌 **WATCHLIST**\n" + "".join(watchlist_reports)
    final_msg += "\n📊 **SECTOR LEADERS**\n"
    for sec, content in sector_reports.items():
        final_msg += f"\n📂 *{sec}*\n{content}"

    send_long_message(final_msg)
    save_state(current_state)
    print("Done.")

if __name__ == "__main__":
    run_analytics_engine()
