from datetime import datetime

from app.models.snapshot import MarketSnapshot
from app.models.stock import StockAnalysis, StockSnapshot
from app.services.telegram import render_daily_brief, send_daily_brief, split_message


def _analysis(ticker):
    snap = StockSnapshot(ticker, f"{ticker} Co", 100, 0.031, 0.05, 0.1, 2.0)
    return StockAnalysis(ticker, f"{ticker} Co", "Positive", "Improving", "Unusual volume", "Medium", "The stock rose on heavier volume.", "The move had stronger participation than normal.", "Earnings could create a sharp move.", "Watch whether the move holds.", [], [], [], [], datetime.utcnow(), snap)


def test_plain_english_brief_max_three_and_no_forbidden_terms():
    market = MarketSnapshot(0.004, 0.006, "Supportive", "The S&P 500 finished higher.")
    message = render_daily_brief(market, [_analysis("AAA"), _analysis("BBB"), _analysis("CCC"), _analysis("DDD")])
    assert message.count("<b>What changed:</b>") == 3
    for forbidden in ["Sharpe", "Value at Risk", "cointegration", "factor exposure"]:
        assert forbidden not in message


def test_split_message_and_dry_run():
    chunks = split_message("a" * 8000, max_length=3900)
    assert len(chunks) == 3
    result = send_daily_brief("hello", dry_run=True)
    assert result["dry_run"] is True
