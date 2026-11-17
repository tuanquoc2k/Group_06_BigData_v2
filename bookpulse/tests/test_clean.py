"""Regression tests for merging CSV ratings with archived stream events."""
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

from batch import clean


class CleanStreamMergeTest(unittest.TestCase):
    def test_csv_and_stream_source_order_columns_merge(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_path = root / "ratings.csv"
            csv_path.write_text(
                "user_id,book_id,rating\n1,10,3\n1,11,5\n",
                encoding="utf-8",
            )
            archive_path = root / "stream_events"
            spark = (SparkSession.builder.master("local[1]").appName("test-clean")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.ui.enabled", "false").getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
            spark.createDataFrame(
                [(1, 10, 5, datetime(2026, 11, 15), datetime(2026, 11, 15),
                  "event-1", 0, False)],
                ("user_id int, book_id int, rating int, event_time timestamp, "
                 "ingested_at timestamp, event_id string, source_order long, simulated boolean"),
            ).write.parquet(str(archive_path))

            output_path = root / "cleaned"
            with patch.object(clean, "get_spark", return_value=spark), \
                    patch.object(clean, "save_report"), \
                    patch.object(sys, "argv", [
                        "clean.py",
                        "--input", str(csv_path),
                        "--output", str(output_path),
                        "--stream-events", str(archive_path),
                    ]):
                clean.main()

            spark = SparkSession.builder.master("local[1]").appName("test-clean-read").getOrCreate()
            rows = {(row.user_id, row.book_id): (row.rating, row.from_stream)
                    for row in spark.read.parquet(str(output_path)).collect()}
            self.assertEqual(rows, {(1, 10): (5, True), (1, 11): (5, False)})
            spark.stop()


if __name__ == "__main__":
    unittest.main()
