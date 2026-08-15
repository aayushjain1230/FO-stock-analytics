from app.navigation import PAGES
from app.services.telegram import render_daily_brief
from app.models.snapshot import MarketSnapshot


def test_four_page_navigation_remains():
    assert PAGES == ["Home", "Stocks", "Opportunities", "Settings"]
    assert all("Lab" not in page and "Portfolio" not in page for page in PAGES)


def test_telegram_remains_one_concise_message_without_forbidden_claims():
    message = render_daily_brief(MarketSnapshot(explanation="Market data unavailable."), [], {})
    assert "Daily Watchlist Intelligence" in message
    assert "BlackRock bought today" not in message
    assert "dark pool" not in message.lower()
