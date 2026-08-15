"""Version 2 orchestration without adding frontend complexity."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from app.analysis.filing_intelligence import FilingInsight, material_filing_insights
from app.analysis.relative_value import analyze_relative_value_candidates
from app.models.factor import FactorModelResult
from app.models.pair_result import RelativeValueResult
from app.models.sec_filing import FilingRecord
from app.models.simulation import SimulationConfig, SimulationResult
from app.models.stock import StockAnalysis
from app.models.volatility import VolatilityForecast
from app.models.whale import WhaleActivityResult
from app.services.bootstrap_simulation import run_historical_bootstrap
from app.services.factor_model import analyze_single_stock_factors
from app.services.market_data import fetch_price_history
from app.services.peer_data import select_peer_candidates
from app.services.risk_metrics import calculate_risk_metrics
from app.services.sec_edgar import SecClientError, SecEdgarClient, parse_form4_xml, parse_schedule_13
from app.services.sec_identity import sec_identity_status
from app.services.volatility_forecast import forecast_volatility
from app.services.whale_activity import analyze_whale_activity
from app.models.risk_metrics import RiskMetricsResult


@dataclass(frozen=True)
class V2StockIntelligence:
    """Combined V2/V3 intelligence for one stock."""

    ticker: str
    filings: list[FilingRecord]
    filing_insights: list[FilingInsight]
    whale_activity: WhaleActivityResult
    simulation: SimulationResult
    relative_values: list[RelativeValueResult]
    volatility: VolatilityForecast
    factor_model: FactorModelResult
    risk_metrics: RiskMetricsResult


def build_v2_intelligence(analyses: list[StockAnalysis], histories: dict[str, pd.DataFrame], market_condition: str | None = None, sync_sec: bool = False) -> dict[str, V2StockIntelligence]:
    """Build SEC-aware V2 intelligence while isolating per-ticker failures."""
    filing_records_by_ticker = fetch_watchlist_filings([item.ticker for item in analyses]) if sync_sec else {item.ticker: [] for item in analyses}
    proxy_histories = fetch_price_history(["SPY", "QQQ", "XLK", "XLY", "XLP", "XLE", "SMH"], period="3y", interval="1d", force_refresh=False)
    candidates = select_peer_candidates([item.ticker for item in analyses], {**histories, **proxy_histories})
    pair_results = analyze_relative_value_candidates(candidates, {**histories, **proxy_histories})
    output: dict[str, V2StockIntelligence] = {}
    for analysis in analyses:
        history = histories.get(analysis.ticker)
        filings = filing_records_by_ticker.get(analysis.ticker, [])
        whale = analyze_whale_activity(analysis.snapshot, history, filings)
        simulation = run_historical_bootstrap(analysis.snapshot, history, SimulationConfig(), market_condition=market_condition)
        related_pairs = [pair for pair in pair_results if analysis.ticker in {pair.ticker_a, pair.ticker_b}]
        volatility = forecast_volatility(analysis.snapshot, history)
        factor = analyze_single_stock_factors(analysis.snapshot, history, proxy_histories.get("SPY"), _sector_proxy_history(analysis.ticker, proxy_histories))
        risk = calculate_risk_metrics(analysis.snapshot, history)
        output[analysis.ticker] = V2StockIntelligence(analysis.ticker, filings, material_filing_insights(filings), whale, simulation, related_pairs, volatility, factor, risk)
    return output


def _sector_proxy_history(ticker: str, proxies: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
    """Return a transparent sector proxy when available."""
    tech = {"AAPL", "MSFT", "GOOGL", "NVDA", "AMD", "TSM", "V", "MA"}
    consumer = {"HD", "LOW", "KO", "PEP"}
    energy = {"XOM", "CVX"}
    if ticker in {"NVDA", "AMD", "TSM"} and "SMH" in proxies:
        return proxies.get("SMH")
    if ticker in tech:
        return proxies.get("XLK") if proxies.get("XLK") is not None else proxies.get("QQQ")
    if ticker in consumer:
        return proxies.get("XLY") if proxies.get("XLY") is not None else proxies.get("XLP")
    if ticker in energy:
        return proxies.get("XLE")
    return proxies.get("SPY")


def fetch_watchlist_filings(tickers: list[str], limit_per_ticker: int = 6) -> dict[str, list[FilingRecord]]:
    """Synchronize a small, respectful set of SEC filings only when configured."""
    if not sec_identity_status().configured:
        return {ticker: [] for ticker in tickers}
    client = SecEdgarClient()
    output: dict[str, list[FilingRecord]] = {}
    for ticker in tickers:
        records: list[FilingRecord] = []
        try:
            metadata = client.recent_filing_metadata(ticker, {"4", "SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"}, limit=limit_per_ticker)
            for item in metadata:
                filing_date = date.fromisoformat(item["filing_date"]) if item.get("filing_date") else date.today()
                if item["form"] == "4":
                    text = client.get_filing_document(item["source_url"])
                    records.extend(parse_form4_xml(text, ticker, item["cik"], item["accession_number"], filing_date, item["source_url"]))
                else:
                    text = client.get_filing_document(item["source_url"])
                    records.append(parse_schedule_13(text, ticker, item["cik"], item["form"], item["accession_number"], filing_date, item["source_url"]))
        except (SecClientError, Exception):
            records = []
        output[ticker] = records
    return output
