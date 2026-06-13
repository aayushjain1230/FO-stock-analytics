import unittest

import pandas as pd

import state_manager


class StateManagerTests(unittest.TestCase):
    def test_event_coverage_uses_state_transitions(self):
        previous_state = {
            "AAPL": {
                "close": 95.0,
                "sma50": 100.0,
                "sma200": 100.0,
                "rsi_weekly": 45.0,
                "rsi_monthly": 35.0,
                "mrs": -1.0,
                "rv": 1.0,
                "high_52w": 100.0,
                "low_52w": 80.0,
                "is_stage_2": False,
            }
        }
        current = pd.DataFrame(
            [
                {
                    "Close": 120.0,
                    "SMA50": 100.0,
                    "SMA200": 100.0,
                    "RSI_Weekly": 39.0,
                    "RSI_Monthly": 41.0,
                    "MRS": 2.0,
                    "RV": 2.5,
                    "High_52W": 120.0,
                    "Low_52W": 80.0,
                }
            ]
        )
        config = {
            "settings": {
                "sma_fast": 50,
                "sma_slow": 200,
                "rsi_weekly_breakdown_threshold": 40,
                "rsi_monthly_breakout_threshold": 40,
                "relative_volume_alert_threshold": 2.0,
            }
        }

        alerts = state_manager.get_ticker_alerts("AAPL", current, previous_state, config=config)

        self.assertIn("Price crossed above SMA50", alerts)
        self.assertIn("Price crossed above SMA200", alerts)
        self.assertIn("Monthly RSI crossed above 40", alerts)
        self.assertIn("Weekly RSI crossed below 40", alerts)
        self.assertIn("New 52-week high", alerts)

    def test_update_state_persists_monthly_and_52_week_fields(self):
        current = pd.DataFrame(
            [
                {
                    "Close": 101.5,
                    "SMA50": 98.2,
                    "SMA200": 90.1,
                    "RSI_Weekly": 55.0,
                    "RSI_Monthly": 44.0,
                    "MRS": 1.2,
                    "RV": 1.8,
                    "High_52W": 110.0,
                    "Low_52W": 70.0,
                }
            ]
        )

        updated = state_manager.update_ticker_state("MSFT", current, {}, config={})

        self.assertEqual(updated["MSFT"]["rsi_monthly"], 44.0)
        self.assertEqual(updated["MSFT"]["high_52w"], 110.0)
        self.assertEqual(updated["MSFT"]["low_52w"], 70.0)


if __name__ == "__main__":
    unittest.main()
