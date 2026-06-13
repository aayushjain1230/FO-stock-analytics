import unittest

import pandas as pd

import why_now


class WhyNowTests(unittest.TestCase):
    def test_no_trigger_suppresses_alert(self):
        analyzed = pd.DataFrame(
            [
                {"Close": 100, "SMA50": 95, "SMA200": 90, "RV": 1.0, "High_52W": 120, "RS_Breakout": False},
                {"Close": 101, "SMA50": 96, "SMA200": 91, "RV": 1.0, "High_52W": 120, "RS_Breakout": False},
            ]
        )
        payload = why_now.evaluate_why_now("AAPL", analyzed, {"final_score": 70}, previous_scores=[], market_payload={})

        self.assertFalse(payload["send_alert"])
        self.assertEqual(payload["reason"], "No clear Why Now trigger")

    def test_relative_strength_breakout_sends_alert(self):
        analyzed = pd.DataFrame(
            [
                {"Close": 100, "SMA50": 105, "SMA200": 90, "RV": 1.0, "High_52W": 120, "RS_Breakout": False},
                {"Close": 106, "SMA50": 105, "SMA200": 90, "RV": 1.2, "High_52W": 120, "RS_Breakout": True},
            ]
        )
        payload = why_now.evaluate_why_now("MSFT", analyzed, {"final_score": 72}, previous_scores=[], market_payload={})

        self.assertTrue(payload["send_alert"])
        self.assertIn("Relative strength", payload["reason"])


if __name__ == "__main__":
    unittest.main()
