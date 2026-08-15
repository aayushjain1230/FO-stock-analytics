import unittest

from engines import strategy_engine


class StrategyEngineTests(unittest.TestCase):
    def test_strategy_validation_summary(self):
        result = strategy_engine.strategy_validation_summary(
            {
                "performance": {"cagr": 0.12},
                "robustness": {"sample_size": 40, "positive_fold_pct": 0.6},
            }
        )
        self.assertIn("signal_reliability", result)


if __name__ == "__main__":
    unittest.main()
