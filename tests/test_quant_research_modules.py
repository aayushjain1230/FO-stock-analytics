import unittest

import numpy as np
import pandas as pd

import alternative_data
import backtesting
import factor_models
import market_regime
import portfolio_engine
import stat_arb


class QuantResearchModuleTests(unittest.TestCase):
    def setUp(self):
        self.rng = np.random.default_rng(11)
        self.index = pd.bdate_range("2022-01-03", periods=600)

    def test_factor_scores_and_portfolio_exposure(self):
        features = pd.DataFrame(
            {
                "return_12m_ex_1m": [0.30, 0.10, -0.10],
                "return_6m": [0.20, 0.05, -0.05],
                "relative_strength": [0.10, 0.0, -0.10],
                "forward_pe": [25, 12, 8],
                "fcf_yield": [0.03, 0.06, 0.08],
                "return_on_equity": [0.30, 0.15, 0.05],
                "revenue_growth": [0.25, 0.08, -0.02],
                "annualized_volatility": [0.25, 0.15, 0.40],
                "maximum_drawdown": [-0.20, -0.10, -0.50],
            },
            index=["AAA", "BBB", "CCC"],
        )
        payload = factor_models.cross_sectional_factor_scores(features)
        exposure = factor_models.portfolio_factor_exposure(payload, {"AAA": 0.7, "BBB": 0.3})
        self.assertTrue(payload["available"])
        self.assertEqual(len(payload["leaderboard"]), 3)
        self.assertTrue(exposure["available"])
        self.assertIn("momentum", exposure["exposures"])

    def test_statistical_regime_includes_probabilities(self):
        path = np.cumsum(self.rng.normal(0.0004, 0.01, len(self.index)))
        spy = pd.DataFrame({"Close": 100 * np.exp(path)}, index=self.index)
        payload = market_regime.detect_statistical_regime(spy)
        self.assertTrue(payload["available"])
        self.assertIn(payload["current_regime"], {"Bull Trend", "Bear Trend", "Sideways", "High Volatility", "Crash", "Recovery"})
        self.assertTrue(payload["transition_probabilities"])

    def test_cointegration_and_walk_forward(self):
        x = np.cumsum(self.rng.normal(0, 0.02, len(self.index))) + 5
        y = 1.2 * x + self.rng.normal(0, 0.05, len(self.index))
        pair = stat_arb.cointegration_score(pd.Series(y, index=self.index), pd.Series(x, index=self.index))
        self.assertTrue(pair["available"])
        self.assertIn("stationarity_test", pair)
        self.assertIn("research_mindset", pair)

        price = pd.Series(100 * np.exp(np.cumsum(self.rng.normal(0.0004, 0.01, len(self.index)))), index=self.index)
        benchmark = pd.Series(100 * np.exp(np.cumsum(self.rng.normal(0.0003, 0.009, len(self.index)))), index=self.index)
        signal = pd.Series(self.rng.normal(size=len(self.index)), index=self.index).rolling(10).mean()
        result = backtesting.walk_forward_signal_backtest(price, signal, benchmark, train_window=252, test_window=63)
        self.assertTrue(result["available"])
        self.assertGreater(len(result["folds"]), 1)
        self.assertIn("performance_attribution", result)

    def test_portfolio_tail_risk_and_alternative_data(self):
        prices = pd.DataFrame(
            {
                "AAA": 100 * np.exp(np.cumsum(self.rng.normal(0.0005, 0.01, len(self.index)))),
                "BBB": 90 * np.exp(np.cumsum(self.rng.normal(0.0003, 0.008, len(self.index)))),
            },
            index=self.index,
        )
        returns = portfolio_engine.portfolio_returns(prices, pd.Series({"AAA": 0.6, "BBB": 0.4}))
        tail = portfolio_engine.downside_and_tail_risk(returns)
        self.assertIn("sortino_ratio", tail)
        self.assertLessEqual(tail["conditional_value_at_risk"], tail["value_at_risk"])

        raw = {
            "google_trends": pd.DataFrame(
                {"AAA": self.rng.normal(size=len(self.index)), "BBB": self.rng.normal(size=len(self.index))},
                index=self.index,
            )
        }
        engineered = alternative_data.engineer_features(raw)
        signal = alternative_data.latest_composite_signal(engineered)
        self.assertTrue(signal["available"])
        self.assertEqual(len(signal["stocks"]), 2)


if __name__ == "__main__":
    unittest.main()
