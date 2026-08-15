import unittest

from engines import risk_engine


class RiskEngineTests(unittest.TestCase):
    def test_risk_brief(self):
        payload = {
            "tail_risk": {"value_at_risk": -0.02, "conditional_value_at_risk": -0.035},
            "maximum_drawdown": -0.12,
            "risk_contributions": {"A": 60, "B": 40},
        }
        result = risk_engine.risk_brief(payload)
        self.assertEqual(result["top_risk_contributors"][0][0], "A")


if __name__ == "__main__":
    unittest.main()
