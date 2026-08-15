import json
from pathlib import Path

import pytest

import main
from app.navigation import PAGES
from app.services.watchlist import MAX_WATCHLIST_SIZE, resolve_watchlist


def test_watchlist_resolution_precedence_and_validation(tmp_path, monkeypatch):
    default = tmp_path / "default_watchlist.json"
    default.write_text(json.dumps(["GOOGL"]), encoding="utf-8")
    monkeypatch.setenv("WATCHLIST_TICKERS", "MSFT,NVDA")

    manual = resolve_watchlist(" aapl,msft,AAPL,bad ticker!,BRK-B ", default_path=default)
    assert manual.tickers == ["AAPL", "MSFT", "BRK-B"]
    assert manual.source == "manual input"
    assert manual.rejected == ["bad ticker!"]

    env = resolve_watchlist(None, default_path=default)
    assert env.tickers == ["MSFT", "NVDA"]
    assert env.source == "WATCHLIST_TICKERS"

    monkeypatch.delenv("WATCHLIST_TICKERS")
    fallback = resolve_watchlist(None, default_path=default)
    assert fallback.tickers == ["GOOGL"]
    assert fallback.source == "config/default_watchlist.json"


def test_watchlist_limit_and_empty_invalid_input(tmp_path, monkeypatch):
    default = tmp_path / "default_watchlist.json"
    default.write_text(json.dumps(["AAPL"]), encoding="utf-8")
    tickers = ",".join(f"A{i}" for i in range(40))
    resolved = resolve_watchlist(tickers, default_path=default)
    assert len(resolved.tickers) == MAX_WATCHLIST_SIZE
    with pytest.raises(ValueError):
        resolve_watchlist("bad ticker!", env_value="", default_path=tmp_path / "missing.json")


def test_export_report_static_label_and_deprecated_dashboard_alias(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    def fake_analyze_watchlist(*args, **kwargs):
        return {
            "summary": "AAPL deserves attention.",
            "resolved_watchlist": {"tickers": ["AAPL"], "source": "manual input", "rejected": []},
            "analyses": [{"ticker": "AAPL", "overall_view": "Worth Watching", "trend": "Improving", "what_changed": "The stock improved."}],
        }

    monkeypatch.setattr(main, "analyze_watchlist", fake_analyze_watchlist)
    path = main.export_static_report(["AAPL"])
    text = path.read_text(encoding="utf-8")
    assert path == Path("reports/watchlist_report.html")
    assert "Static Watchlist Report" in text
    assert "streamlit run streamlit_app.py" in text


def test_workflow_manual_inputs_and_no_state_commits():
    workflow = Path(".github/workflows/main.yml").read_text(encoding="utf-8")
    assert "watchlist:" in workflow
    assert "send_telegram:" in workflow
    assert "sync_sec:" in workflow
    assert "export_report:" in workflow
    assert "WATCHLIST_TICKERS: ${{ vars.WATCHLIST_TICKERS }}" in workflow
    assert "git commit" not in workflow
    assert "*/5" not in workflow
    assert "reports/watchlist_report.html" in workflow


def test_background_workflow_is_accurately_named_and_bounded():
    assert not Path(".github/workflows/v2-background-jobs.yml").exists()
    workflow = Path(".github/workflows/background-jobs.yml").read_text(encoding="utf-8")
    assert "Stock Intelligence Background Jobs" in workflow
    assert "python main.py --model-report" in workflow
    assert "python main.py --analyze" in workflow
    assert "git commit" not in workflow


def test_streamlit_official_entry_and_no_lab_navigation():
    streamlit = Path("streamlit_app.py").read_text(encoding="utf-8")
    assert "sync_sec=False" not in streamlit
    assert "Sync SEC filings" in streamlit
    assert PAGES == ["Home", "Stocks", "Opportunities", "Settings"]
    assert all("Lab" not in page and "Portfolio" not in page for page in PAGES)


def test_user_facing_copy_has_no_phase_labels_or_legacy_dashboard_claims():
    files = [
        Path("README.md"),
        Path("app/ui/pages/home.py"),
        Path("app/ui/pages/stocks.py"),
        Path("app/ui/pages/opportunities.py"),
        Path("app/ui/pages/settings.py"),
        Path("streamlit_app.py"),
    ]
    text = "\n".join(path.read_text(encoding="utf-8") for path in files)
    for stale in ["Version 1 opportunities", "Version 2 app", "Version 3 background jobs", "static HTML as the main dashboard"]:
        assert stale not in text
    assert "streamlit run streamlit_app.py" in Path("README.md").read_text(encoding="utf-8")


def test_legacy_frontend_not_imported_by_official_app():
    streamlit = Path("streamlit_app.py").read_text(encoding="utf-8")
    assert "frontend" not in streamlit
    assert "quant_dashboard" not in streamlit
