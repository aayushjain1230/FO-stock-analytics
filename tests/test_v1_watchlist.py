from app.services import watchlist


def test_add_remove_normalize_and_prevent_duplicates():
    tickers = ["AAPL"]
    updated, error = watchlist.add_ticker(tickers, " msft ")
    assert error is None
    assert updated == ["AAPL", "MSFT"]
    duplicate, error = watchlist.add_ticker(updated, "MSFT")
    assert duplicate == updated
    assert "already" in error
    assert watchlist.remove_ticker(updated, "aapl") == ["MSFT"]


def test_reject_invalid_ticker():
    updated, error = watchlist.add_ticker([], "BAD TICKER!")
    assert updated == []
    assert "not a valid" in error


def test_enforce_size_limit():
    tickers = [f"A{i}" for i in range(watchlist.MAX_WATCHLIST_SIZE)]
    updated, error = watchlist.add_ticker(tickers, "ZZZ")
    assert updated == tickers
    assert "limit" in error


def test_session_isolation_by_copy():
    first = watchlist.load_default_watchlist()
    second = list(first)
    first_added, _ = watchlist.add_ticker(first, "TSLA")
    assert "TSLA" in first_added
    assert "TSLA" not in second
