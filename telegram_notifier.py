import json
import os
import re
import time
from datetime import datetime
from html import escape

try:
    import requests
except ModuleNotFoundError:
    requests = None


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


def _safe_pct(value, multiply=True):
    value = _safe_float(value)
    if value is None:
        return "N/A"
    if multiply:
        value *= 100
    return f"{value:+.2f}%" if value > 0 else f"{value:.2f}%"


def _html(value):
    return escape(str(value if value is not None else "N/A"), quote=False)


class TelegramMessageBuilder:
    """Build concise Telegram HTML briefings for portfolio and quant research.

    The builder keeps every section optional, safely escapes HTML, and splits
    detailed reports into Telegram-safe mobile-sized chunks.
    """

    TELEGRAM_LIMIT = 4096
    SOFT_LIMIT = 3400
    DIVIDER = "━━━━━━━━━━━━━━━━━━"
    SEVERITY = {
        "strong": "🟢",
        "good": "🟢",
        "neutral": "🟡",
        "watch": "🟡",
        "risk": "🔴",
        "problem": "🔴",
        "info": "🔵",
        "warning": "⚠️",
        "urgent": "🚨",
    }

    def __init__(self, quant_payload=None, portfolio_payload=None):
        self.quant = quant_payload or {}
        self.portfolio = portfolio_payload or {}

    def status_icon(self, severity):
        return self.SEVERITY.get(str(severity).lower(), "🔵")

    def _health_status(self):
        score = _safe_float(self.portfolio.get("portfolio_health", {}).get("score"))
        if score is None:
            return "info", "Unknown"
        if score >= 80:
            return "good", "Strong"
        if score >= 60:
            return "watch", "Watch"
        return "risk", "Risk"

    def _portfolio_health(self):
        return self.portfolio.get("portfolio_health", {}).get("score", "N/A")

    def _main_risk(self):
        warnings = self.portfolio.get("risk_warnings", [])
        if warnings:
            return str(warnings[0])
        top_risk = sorted(
            self.portfolio.get("risk_contributions", {}).items(),
            key=lambda item: _safe_float(item[1]) or 0,
            reverse=True,
        )
        if top_risk:
            return f"{top_risk[0][0]} contributes {top_risk[0][1]:.1f}% of portfolio risk"
        return "No dominant risk flagged"

    def build_header(self):
        severity, status = self._health_status()
        regime = self.quant.get("market_regime", {})
        regime_text = regime.get("regime") or regime.get("current_regime") or "Unknown"
        vol_regime = self.portfolio.get("variance", {}).get("risk_classification")
        if vol_regime:
            regime_text = f"{regime_text}, {vol_regime} Volatility"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        return "\n".join(
            [
                "🚨 <b>PORTFOLIO INTELLIGENCE REPORT</b>",
                _html(timestamp),
                "",
                f"<b>Overall Status:</b> {self.status_icon(severity)} {_html(status)}",
                f"<b>Portfolio Health:</b> {_html(self._portfolio_health())}/100",
                f"<b>Market Regime:</b> {_html(regime_text)}",
                f"<b>Main Risk:</b> {_html(self._main_risk())}",
            ]
        )

    def build_portfolio_snapshot(self):
        if not self.portfolio:
            return ""
        variance = self.portfolio.get("variance", {})
        sharpe = self.portfolio.get("sharpe", {})
        drawdown = self.portfolio.get("maximum_drawdown")
        vol = variance.get("annual_volatility")
        sharpe_ratio = _safe_float(sharpe.get("sharpe_ratio"))
        interpretation = "Portfolio is stable, but monitor concentration and regime changes."
        if _safe_float(vol) and _safe_float(vol) >= 0.25:
            interpretation = "Portfolio is participating, but volatility is elevated and position sizing matters."
        if sharpe_ratio is not None and sharpe_ratio < 0.5:
            interpretation = "Risk-adjusted returns are weak; focus on drawdown, concentration, and signal quality."
        lines = [
            "📊 <b>PORTFOLIO SNAPSHOT</b>",
            f"Return: {_html(_safe_pct(self.portfolio.get('portfolio_return')))}",
            f"Portfolio Volatility: {_html(_safe_pct(vol))}",
            f"Sharpe Ratio: {_html(f'{sharpe_ratio:.2f}' if sharpe_ratio is not None else 'N/A')}",
            f"Max Drawdown: {_html(_safe_pct(drawdown))}",
            f"Cash: {_html(_safe_pct(self.portfolio.get('cash_allocation'), multiply=False))}",
            "",
            "<b>Interpretation:</b>",
            _html(interpretation),
        ]
        return "\n".join(lines)

    def build_top_risks(self):
        risks = []
        for warning in self.portfolio.get("risk_warnings", [])[:4]:
            icon = "🔴" if any(term in str(warning).lower() for term in ["cvar", "risk", "volatility", "correlation"]) else "🟡"
            risks.append(f"{icon} {_html(warning)}")
        risk_contrib = self.portfolio.get("risk_contributions", {})
        if not risks and risk_contrib:
            for ticker, value in sorted(risk_contrib.items(), key=lambda item: item[1], reverse=True)[:3]:
                icon = "🔴" if value >= 30 else "🟡"
                risks.append(f"{icon} {_html(ticker)} contributes {_html(f'{value:.1f}%')} of portfolio risk")
        avg_corr = _safe_float(self.portfolio.get("correlation", {}).get("average_correlation"))
        if avg_corr is not None and avg_corr >= 0.65:
            risks.append(f"🟡 Average correlation is elevated at {_html(f'{avg_corr:.2f}')}")
        if not risks:
            return ""
        return "\n".join(
            [
                "⚠️ <b>TOP RISKS</b>",
                *risks[:5],
                "",
                "<b>What this means:</b>",
                "Holdings may be moving together, which can reduce diversification when markets turn.",
            ]
        )

    def build_factor_exposure(self):
        exposures = self.quant.get("portfolio_factor_exposure", {}).get("exposures", {})
        if not exposures:
            exposures = self.portfolio.get("factor_exposure", {}).get("main_risk_drivers", {})
        if not exposures:
            return ""
        rows = []
        for name, value in sorted(exposures.items(), key=lambda item: item[1], reverse=True)[:6]:
            icon = "🟢" if value >= 65 else "🔴" if value <= 35 else "🟡"
            label = "Strong" if value >= 65 else "Weak" if value <= 35 else "Neutral"
            rows.append(f"{_html(str(name).replace('_', ' ').title())}: {icon} {_html(label)}")
        strongest = max(exposures.items(), key=lambda item: item[1], default=("N/A", 0))[0]
        return "\n".join(
            [
                "🧠 <b>FACTOR EXPOSURE</b>",
                *rows,
                "",
                "<b>Main driver:</b>",
                f"Portfolio behavior is most tilted toward {_html(str(strongest).replace('_', ' '))}.",
            ]
        )

    def build_stock_signals(self):
        tickers = sorted(
            self.quant.get("tickers", []),
            key=lambda row: _safe_float(row.get("quant_score") or row.get("score")) or 0,
            reverse=True,
        )[:3]
        if not tickers:
            return ""
        lines = ["📈 <b>TOP STOCK SIGNALS</b>"]
        for idx, row in enumerate(tickers, 1):
            score = _safe_float(row.get("quant_score") or row.get("score")) or 0
            confidence = _safe_float(row.get("confidence")) or 0
            icon = "🟢" if score >= 70 else "🔴" if score < 55 else "🟡"
            reason = row.get("why_now")
            if not reason or reason == "No clear Why Now trigger":
                reason = row.get("research_note", "Signal is based on score, risk, and factor alignment.")
            lines.extend(
                [
                    f"{idx}. {_html(row.get('ticker'))} — {icon} Score {_html(f'{score:.0f}')} | Confidence {_html(f'{confidence:.0f}%')}",
                    f"Reason: {_html(str(reason)[:130])}",
                    "",
                ]
            )
        return "\n".join(lines).rstrip()

    def build_quant_alerts(self):
        lines = []
        pairs = self.quant.get("pairs_trading", {}).get("candidates", [])
        if pairs:
            pair = pairs[0]
            z_value = _safe_float(pair.get("spread_zscore")) or 0
            lines.extend(
                [
                    f"🟢 New cointegration opportunity: {_html(pair.get('pair'))}",
                    f"Z-Score: {_html(f'{z_value:.2f}')}",
                    f"Signal: {_html(pair.get('signal', {}).get('action', 'Watch for mean reversion'))}",
                ]
            )
        transitions = self.quant.get("market_regime", {}).get("transition_probabilities", {})
        if transitions:
            name, probability = max(transitions.items(), key=lambda item: item[1])
            lines.extend(["", f"🟡 Regime transition risk: {_html(name)} probability {_html(f'{probability:.1f}%')}"])
        if not lines:
            return ""
        return "\n".join(["🔬 <b>QUANT RESEARCH ALERTS</b>", *lines])

    def build_action_watchlist(self):
        actions = []
        top_risk = sorted(
            self.portfolio.get("risk_contributions", {}).items(),
            key=lambda item: item[1],
            reverse=True,
        )
        if top_risk:
            actions.append(f"✅ Watch {top_risk[0][0]} risk contribution")
        if _safe_float(self.portfolio.get("correlation", {}).get("average_correlation")) and _safe_float(self.portfolio.get("correlation", {}).get("average_correlation")) >= 0.60:
            actions.append("✅ Monitor portfolio correlation")
        if self.quant.get("pairs_trading", {}).get("candidates"):
            actions.append("✅ Recheck pair spread and z-score")
        actions.append("✅ Avoid adding highly correlated exposure without a thesis")
        return "\n".join(["🎯 <b>ACTION WATCHLIST</b>", *actions[:5]])

    def build_footer(self):
        confidences = [_safe_float(row.get("confidence")) for row in self.quant.get("tickers", [])]
        confidences = [value for value in confidences if value is not None]
        confidence = sum(confidences) / len(confidences) if confidences else 70
        data_quality = "Good" if confidence >= 60 else "Watch"
        return "\n".join(
            [
                f"<b>Confidence:</b> {_html(f'{confidence:.0f}%')}",
                f"<b>Data Quality:</b> {_html(data_quality)}",
                "<b>Next Update:</b> 30 minutes",
            ]
        )

    def build_summary(self):
        severity, status = self._health_status()
        return "\n".join(
            [
                "🚨 <b>PORTFOLIO BRIEFING</b>",
                f"<b>Status:</b> {self.status_icon(severity)} {_html(status)} | <b>Health:</b> {_html(self._portfolio_health())}/100",
                f"<b>Main Risk:</b> {_html(self._main_risk())}",
                f"<b>Watch:</b> {_html(self.build_action_watchlist().splitlines()[1] if len(self.build_action_watchlist().splitlines()) > 1 else 'Review portfolio risk dashboard')}",
            ]
        )

    def build_detailed_report(self):
        sections = [
            self.build_header(),
            self.build_portfolio_snapshot(),
            self.build_top_risks(),
            self.build_factor_exposure(),
            self.build_stock_signals(),
            self.build_quant_alerts(),
            self.build_action_watchlist(),
            self.build_footer(),
        ]
        return f"\n\n{self.DIVIDER}\n\n".join(section for section in sections if section)

    def split_messages(self, text):
        chunks = []
        remaining = text
        while len(remaining) > self.SOFT_LIMIT:
            split_at = remaining.rfind(f"\n\n{self.DIVIDER}\n\n", 0, self.SOFT_LIMIT)
            if split_at <= 0:
                split_at = remaining.rfind("\n\n", 0, self.SOFT_LIMIT)
            if split_at <= 0:
                split_at = self.SOFT_LIMIT
            chunks.append(remaining[:split_at].strip())
            remaining = remaining[split_at:].strip()
        if remaining:
            chunks.append(remaining)
        return chunks

    def inline_keyboard(self):
        return {
            "inline_keyboard": [
                [
                    {"text": "📊 Portfolio", "callback_data": "portfolio"},
                    {"text": "⚠️ Risks", "callback_data": "risks"},
                ],
                [
                    {"text": "🔬 Research", "callback_data": "research"},
                    {"text": "📈 Top Stocks", "callback_data": "top_stocks"},
                ],
            ]
        }


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


def _execute_send(url, chat_id, text, retries=3, parse_mode="MarkdownV2", reply_markup=None):
    if requests is None:
        print("Telegram Failure: requests is not installed.")
        return False
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

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


def send_long_message(message_text, parse_mode="MarkdownV2", reply_markup=None):
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
            _execute_send(url, chat_id, message_text, parse_mode=parse_mode, reply_markup=reply_markup)
            break

        split_at = message_text.rfind("\n", 0, max_length)
        if split_at == -1:
            split_at = max_length

        chunk = message_text[:split_at]
        _execute_send(url, chat_id, chunk, parse_mode=parse_mode, reply_markup=reply_markup)
        reply_markup = None
        message_text = message_text[split_at:].lstrip()
        time.sleep(0.8)


def send_quant_intelligence_report(quant_payload, portfolio_payload=None):
    """Send a summary first, then clean Telegram HTML detail chunks."""
    builder = TelegramMessageBuilder(quant_payload, portfolio_payload or {})
    send_long_message(builder.build_summary(), parse_mode="HTML", reply_markup=builder.inline_keyboard())
    time.sleep(0.5)
    for chunk in builder.split_messages(builder.build_detailed_report()):
        send_long_message(chunk, parse_mode="HTML")
        time.sleep(0.5)


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


def format_quant_intelligence_report(quant_payload, portfolio_payload=None):
    return TelegramMessageBuilder(quant_payload, portfolio_payload or {}).build_detailed_report()
