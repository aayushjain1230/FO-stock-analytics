# Methodology Version 2

This application is a research tool, not investment advice. Version 2 adds SEC filing intelligence, cautious Whale Activity analysis, historical-bootstrap scenarios, event-aware risk warnings, and calibration checks.

## SEC filing intelligence

The app uses SEC EDGAR structured company submissions and source filing documents. SEC requests require `SEC_USER_AGENT` with an application name and contact email. If it is missing, SEC sync is disabled gracefully.

- Form 4: insider transaction disclosures. Open-market purchases and sales are separated from option exercises, grants, gifts, and tax withholding.
- Schedule 13D: major-holder ownership above 5%; may include control-related purpose language, but intent must be read from the filing.
- Schedule 13G: major-holder ownership above 5% generally used for passive or qualifying ownership; it is not an activist signal by itself.
- Form 13F: delayed quarterly ownership-trend evidence from tracked managers. It does not prove what a manager owns or bought today.

All important filing interpretations keep source SEC links.

## Whale Activity

Whale Activity estimates whether trading behavior is consistent with larger buyers or sellers. It cannot identify anonymous market participants or prove intentions.

Components include volume anomaly, price response, closing pressure when intraday data is available, VWAP behavior when intraday data is available, multi-day accumulation, filing confirmation, and signal reliability.

The engine checks confounders such as nearby earnings and unusually large event-like moves. If data is weak or confounded, the engine can return `Insufficient Data`.

## Historical-bootstrap simulations

The simulation engine samples historical returns to create conditional model scenarios. Standard bootstrap, block bootstrap, and regime-conditioned sampling are supported. Outputs include median ending price, percentile ranges, upside/downside scenario frequencies, drawdown distribution, model disagreement, data coverage, and confidence.

These are not guaranteed real-world probabilities. The correct wording is “across historical-return scenarios,” not “there is definitely an X% chance.”

## Event-aware risk

If earnings are close, ordinary daily-return simulations are marked event-exposed. The app lowers confidence unless there is a separate validated event methodology.

## Calibration

Walk-forward calibration uses only data available at each historical forecast date, then compares forecast intervals with the realized outcome. It measures interval coverage, directional hit rate, Brier score, calibration error, median forecast error, false-confidence rate, and simple baselines.

## Confidence

Confidence considers price-data completeness, intraday completeness, fundamental freshness, filing freshness, independent evidence categories, contradicting evidence, model agreement, calibration sample size, earnings proximity, confounders, and structural market changes. Related price indicators are not counted as separate independent confirmations.

## Persistence

Runtime data is stored under `state/` and `cache/`. Local SQLite is acceptable for scheduled single-process operation, but Streamlit Community Cloud local storage should not be treated as permanent. Runtime databases and caches should not be committed.

## Deferred to Version 3

Cointegration, pairs trading, GARCH, advanced volatility modeling, broad factor models, and full model comparison remain deferred.
