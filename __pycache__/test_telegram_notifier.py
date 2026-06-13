import unittest

import pandas as pd

import telegram_notifier


class TelegramNotifierTests(unittest.TestCase):
    def test_ticker_report_is_bundled_and_sectioned(self):
        latest = pd.Series({"Close": 123.45, "SMA200": 120.0})
        rating = {
            "score": 88,
            "rating": "Tier 1: Market Leader",
            "metrics": {
                "close": 123.45,
                "weekly_rsi": 58.2,
                "monthly_rsi": 61.7,
                "mrs_value": 3.4,
            },
        }

        report = telegram_notifier.format_ticker_report(
            "AAPL",
            ["Price crossed above SMA200", "Monthly RSI crossed above 40"],
            latest,
            rating,
            daily_change=2.35,
        )

        self.assertIn("*Triggered events*", report)
        self.assertIn("*Snapshot metrics*", report)
        self.assertEqual(report.count("*AAPL*"), 1)
        self.assertIn("Price crossed above SMA200", report)
        self.assertIn("Monthly RSI", report)
        self.assertIn("RS Status", report)
        self.assertIn("Up", report)


if __name__ == "__main__":
    unittest.main()
