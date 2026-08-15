from app.navigation import PAGES


def test_navigation_has_exactly_four_destinations():
    assert PAGES == ["Home", "Stocks", "Opportunities", "Settings"]


def test_streamlit_app_imports():
    import streamlit_app

    assert callable(streamlit_app.main)
