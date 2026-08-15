import unittest

from engines import ev_engine


class EVEngineTests(unittest.TestCase):
    def test_simple_expected_value(self):
        result = ev_engine.simple_expected_value(0.6, 2.0, -1.0)
        self.assertAlmostEqual(result["result"], 0.8)
        self.assertIn("formula_used", result)
        self.assertIn("plain_english_explanation", result)

    def test_weighted_expected_value_requires_probabilities_sum_to_one(self):
        with self.assertRaises(ValueError):
            ev_engine.weighted_expected_value([
                {"probability": 0.7, "payoff": 1.0},
                {"probability": 0.4, "payoff": -1.0},
            ])

    def test_portfolio_expected_value(self):
        result = ev_engine.portfolio_expected_value({"A": 0.6, "B": 0.4}, {"A": 0.10, "B": 0.05})
        self.assertAlmostEqual(result["result"], 0.08)


if __name__ == "__main__":
    unittest.main()
