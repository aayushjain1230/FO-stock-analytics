# Methodology Version 3

Version 3 adds advanced quantitative research while keeping the product watchlist-first. The app still answers: what changed, why it matters, main risk, and what to watch next.

## Plain-English layer

Advanced models are backend evidence. The main UI avoids equations and trade instructions. Technical details stay collapsed.

## Correlation versus cointegration

Correlation measures whether two return series tend to move together. Cointegration asks whether two price series maintain a more persistent long-run relationship. Correlation alone cannot qualify a pair.

## Economic peer selection

Pairs are selected from a documented peer universe before statistical testing. Reasons include same or related industry, similar business model, similar revenue drivers, similar market capitalization/liquidity context, and sufficient overlapping history. If economic peer information is unavailable, the app does not mine arbitrary pairs.

## Engle-Granger-style testing

The pair engine aligns adjusted close prices, uses log prices, estimates a hedge relationship with an intercept, constructs a spread, checks residual mean reversion, estimates half-life, and measures current spread distance. A single p-value is not enough.

## Multiple-testing correction

When multiple candidate pairs are evaluated, Benjamini-Hochberg false-discovery control is applied. Raw and adjusted values are kept in technical details.

## Rolling stability and structural breaks

The engine checks rolling stationarity, rolling hedge-ratio stability, and a structural-break proxy. Structural breaks can invalidate relative-value evidence.

## Relative-value language

The app may say one stock is trading below its historical relationship with a peer. It must not say the stock is fundamentally undervalued, convergence is guaranteed, or a pair trade should be executed.

## Walk-forward pair validation

Validation fits relationships on historical training windows and checks later test windows. It records narrowing rate, adverse movement, survival rate, and transaction-cost warnings. This is relationship research, not a live strategy track record.

## EWMA versus GARCH

EWMA volatility is transparent and responsive to recent changes. GARCH is only used when the optional dependency is available and sanity checks pass. GARCH is not automatically better; failed or near-integrated models fall back to EWMA.

## Regime-aware simulations

Bootstrap simulations keep standard, block, and regime-conditioned methods. Percentages are conditional model-scenario outputs, not guaranteed probabilities.

## Factor exposure

Single-stock factor analysis uses transparent market and sector proxies when available. Unsupported factors are skipped. Regression residuals are not called alpha unless strongly qualified.

## Sharpe and Sortino

Sharpe, Sortino, downside deviation, drawdown, consistency, and recovery time are descriptive metrics over multiple windows. They are not predictions or buy signals.

## Evidence aggregation

Evidence categories remain separate: business quality, price behavior, Whale Activity, filing evidence, relative value, simulation outlook, downside risk, market environment, data quality, and model reliability. Failed models contribute nothing. Contradicting evidence remains visible.

## Generated methodology report

Run:

```bash
python main.py --model-report
```

The generated private report is written under `reports/` and ignored by git. It lists model inventory, versions, validation status, active/disabled models, baseline comparisons, and known weaknesses.

## Why the app does not execute trades

The app is for research discipline and evidence review. It does not ask for holdings, shares, cost basis, broker credentials, or account balances, and it does not execute trades.
