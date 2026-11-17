"""Unit tests for latency summaries and fold-in ranking diagnostics."""
import unittest

from scripts.diagnose_foldin import top10_change
from scripts.measure_latency import as_utc, describe


class LatencySummaryTest(unittest.TestCase):
    def test_reports_measured_percentiles(self):
        summary = describe([0.1, 0.2, 0.3])

        self.assertEqual(summary["count"], 3)
        self.assertAlmostEqual(summary["mean_seconds"], 0.2)
        self.assertAlmostEqual(summary["p95_seconds"], 0.29)
        self.assertAlmostEqual(summary["max_seconds"], 0.3)

    def test_empty_latency_sample_is_explicit(self):
        self.assertEqual(describe([])["count"], 0)
        self.assertIsNone(describe([])["p95_seconds"])

    def test_database_naive_utc_timestamps_are_normalized(self):
        from datetime import datetime, timezone

        naive = datetime(2026, 11, 16, 5, 0)
        aware = as_utc(naive)

        self.assertEqual(aware.tzinfo, timezone.utc)
        self.assertEqual(aware, datetime(2026, 11, 16, 5, 0, tzinfo=timezone.utc))


class TopTenChangeTest(unittest.TestCase):
    def test_compares_recommendation_membership_and_rank(self):
        before = [{"book_id": book_id} for book_id in (1, 2, 3)]
        after = [{"book_id": book_id} for book_id in (2, 4, 1)]

        result = top10_change(before, after)

        self.assertEqual(result["before"], [1, 2, 3])
        self.assertEqual(result["after"], [2, 4, 1])
        self.assertEqual(result["jaccard"], 0.5)
        self.assertEqual(result["mean_absolute_rank_shift"], 1.5)


if __name__ == "__main__":
    unittest.main()
