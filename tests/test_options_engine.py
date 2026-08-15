import unittest

from engines import options_engine


class OptionsEngineTests(unittest.TestCase):
    def test_breakevens(self):
        self.assertEqual(options_engine.breakeven_call(100, 5)["breakeven"], 105)
        self.assertEqual(options_engine.breakeven_put(100, 5)["breakeven"], 95)


if __name__ == "__main__":
    unittest.main()
