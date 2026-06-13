import unittest

import pandas as pd

import portfolio_engine


class PortfolioEngineTests(unittest.TestCase):
    def setUp(self):
        index = pd.date_range("2025-01-01", periods=90, freq="B")
        self.prices = pd.DataFrame(
            {
                "AAA": [100 + i * 0.30 for i in range(90)],
                "BBB": [80 + i * 0.10 + ((-1) ** i) * 0.4 for i in range(90)],
                "CCC": [50 + i * 0.05 for i in range(90)],
            },
            index=index,
        )
        self.weights = pd.Series({"AAA": 0.5, "BBB": 0.3, "CCC": 0.2})

    def test_variance_and_volatility_are_positive(self):
        payload = portfolio_engine.portfolio_variance_metrics(self.prices, self.weights)

        self.assertGreaterEqual(payload["daily_variance"], 0)
        self.assertGreaterEqual(payload["annual_volatility"], 0)
        self.assertIn(payload["risk_classification"], {"Very Low", "Low", "Moderate", "High", "Very High"})

    def test_risk_contributions_sum_near_100(self):
        contributions = portfolio_engine.risk_contributions(self.prices, self.weights)

        self.assertAlmostEqual(sum(contributions.values()), 100, delta=0.5)

    def test_generate_report_contains_health_and_why_now(self):
        price_data = {ticker: self.prices[[ticker]].rename(columns={ticker: "Close"}) for ticker in self.prices.columns}
        positions = [
            {"ticker": "AAA", "weight": 0.5, "sector": "Technology"},
            {"ticker": "BBB", "weight": 0.3, "sector": "Financials"},
            {"ticker": "CCC", "weight": 0.2, "sector": "Energy"},
        ]

        report = portfolio_engine.generate_portfolio_report(positions, price_data, risk_free_rate=0.0)

        self.assertIn("portfolio_health", report)
        self.assertIn("why_now", report)
        self.assertIn("risk_contributions", report)


if __name__ == "__main__":
    unittest.main()
