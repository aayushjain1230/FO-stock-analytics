import unittest

from engines import probability_engine


class ProbabilityEngineTests(unittest.TestCase):
    def test_bayes_update(self):
        result = probability_engine.bayes_update(0.5, 0.8, 0.2)
        self.assertAlmostEqual(result["result"], 0.8)
        self.assertIn("formula_used", result)

    def test_binomial_probability(self):
        result = probability_engine.binomial_probability(0.5, 2, 4)
        self.assertAlmostEqual(result["result"], 0.375)

    def test_confidence_score_bounds(self):
        result = probability_engine.confidence_score(100, 1.0, 1.0)
        self.assertAlmostEqual(result["result"], 1.0)


if __name__ == "__main__":
    unittest.main()
