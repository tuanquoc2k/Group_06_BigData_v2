"""Unit tests for the observability part of streaming/stream_stats.py (ProgressLog).

ProgressLog is polled every 10 seconds and reads recentProgress, which repeats batches it has already
returned; totals must count every batch exactly once.
    docker compose exec spark python -m unittest tests.test_stream_progress -v
"""
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from streaming.stream_stats import (
    ProgressLog, find_lost_offsets, latest_event_by_user, latest_rows_by_pair, record_offset_gaps,
)


class FakeQuery:
    def __init__(self, progress):
        self.id, self.recentProgress = "q-1", progress


def batch(batch_id, rows, dropped_late=0, dropped_dups=0, watermark="2026-11-06T12:00:00.000Z"):
    return {"batchId": batch_id, "numInputRows": rows, "inputRowsPerSecond": rows / 5,
            "processedRowsPerSecond": rows / 2, "durationMs": {"triggerExecution": 2000},
            "eventTime": {"watermark": watermark},
            "stateOperators": [{"numRowsTotal": 100 + batch_id, "numRowsDroppedByWatermark": dropped_late,
                                "customMetrics": {"numDroppedDuplicateRows": dropped_dups}}]}


class ProgressLogTest(unittest.TestCase):
    def test_counts_each_batch_once_across_polls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "progress.json"
            log = ProgressLog(path, {"watermark": "10 minutes"})
            query = FakeQuery([batch(0, 50), batch(1, 40, dropped_late=2)])
            log.collect({"valid": query})
            query.recentProgress = [batch(1, 40, dropped_late=2), batch(2, 30, dropped_late=1, dropped_dups=3)]
            log.collect({"valid": query})          # batch 1 is seen a second time
            log.write({"valid": query})
            q = json.loads(path.read_text(encoding="utf-8"))["queries"]["valid"]
        self.assertEqual(q["batches"], 3)
        self.assertEqual(q["input_rows"], 120)
        self.assertEqual(q["rows_dropped_by_watermark"], 3)
        self.assertEqual(q["duplicates_dropped"], 3)
        self.assertEqual(q["state_rows_now"], 102)
        self.assertEqual(q["max_batch_duration_ms"], 2000)


class LatestRatingRowsTest(unittest.TestCase):
    def test_coalesces_repeated_user_book_ratings_to_newest_event(self):
        start = datetime(2026, 11, 16, tzinfo=timezone.utc)

        def row(rating, event_seconds, ingested_seconds, event_id):
            return {
                "user_id": 7,
                "book_id": 42,
                "rating": rating,
                "event_time": start + timedelta(seconds=event_seconds),
                "ingested_at": start + timedelta(seconds=ingested_seconds),
                "event_id": event_id,
            }

        latest = latest_rows_by_pair([
            row(5, 1, 2, "first"),
            row(2, 3, 4, "later"),
            row(4, 3, 5, "latest-tie"),
        ])

        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0]["rating"], 4)
        self.assertEqual(latest[0]["event_id"], "latest-tie")

    def test_builds_one_latest_worker_trigger_per_user(self):
        start = datetime(2026, 11, 16, tzinfo=timezone.utc)
        rows = [
            {"user_id": 7, "event_time": start, "ingested_at": start, "event_id": "older"},
            {"user_id": 7, "event_time": start + timedelta(seconds=1),
             "ingested_at": start + timedelta(seconds=1), "event_id": "newer"},
            {"user_id": 8, "event_time": start, "ingested_at": start, "event_id": "other-user"},
        ]

        latest = latest_event_by_user(rows)

        self.assertEqual(set(latest), {7, 8})
        self.assertEqual(latest[7][2], "newer")
        self.assertEqual(latest[8][2], "other-user")


class KafkaOffsetLossTest(unittest.TestCase):
    def test_detects_retention_gaps_per_partition(self):
        gaps = find_lost_offsets(
            {"0": 10, "1": 40, "2": 75},
            {"0": 10, "1": 44, "2": 70},
        )
        self.assertEqual(gaps, {
            "1": {
                "requested_offset": 40,
                "earliest_available_offset": 44,
                "lost_records": 4,
            },
        })

    def test_persists_each_gap_with_query_and_batch_identity(self):
        class State:
            def __init__(self):
                self.docs = {}

            def update_one(self, query, update, upsert=False):
                self.docs.setdefault(query["_id"], update["$setOnInsert"])

        db = type("FakeDb", (), {"stream_state": State()})()
        progress = {
            "id": "query-1",
            "batchId": 9,
            "sources": [{
                "startOffset": {"rating-events": {"0": 12}},
            }],
        }
        first = record_offset_gaps(db, progress, {"0": 20})
        second = record_offset_gaps(db, progress, {"0": 20})

        self.assertEqual(first["0"]["lost_records"], 8)
        self.assertEqual(second, first)
        self.assertEqual(len(db.stream_state.docs), 1)
        saved = next(iter(db.stream_state.docs.values()))
        self.assertEqual(saved["query_id"], "query-1")
        self.assertEqual(saved["batch_id"], 9)


class StreamingLoggerTest(unittest.TestCase):
    def test_streaming_module_uses_module_logger_for_restart_errors(self):
        import logging
        import streaming.stream_stats as stream_stats

        self.assertIsInstance(stream_stats.logger, logging.Logger)
        self.assertTrue(callable(stream_stats.logger.exception))


class FakeStateCollection:
    def __init__(self, doc):
        self.doc = doc

    def find_one(self, _query):
        return self.doc


class FakeDb:
    def __init__(self, last_batch=None):
        self.stream_state = FakeStateCollection(None if last_batch is None else {"last_batch": last_batch})


class AlreadyAppliedTest(unittest.TestCase):
    """stream_state.last_batch makes the non-idempotent $inc writes safe when Spark replays a batch."""

    def test_new_batch_is_applied(self):
        from streaming.stream_stats import already_applied
        self.assertFalse(already_applied(FakeDb(), "valid_events", 0))
        self.assertFalse(already_applied(FakeDb(last_batch=4), "valid_events", 5))

    def test_replayed_batch_is_skipped(self):
        from streaming.stream_stats import already_applied
        self.assertTrue(already_applied(FakeDb(last_batch=5), "valid_events", 5))

    def test_checkpoint_mismatch_stops_instead_of_skipping(self):
        # checkpoint deleted but MongoDB kept: batch ids restart at 0 while last_batch is 57
        from streaming.stream_stats import already_applied
        with self.assertRaises(RuntimeError):
            already_applied(FakeDb(last_batch=57), "valid_events", 0)

    def test_mongodb_reset_but_checkpoint_kept_stops(self):
        # stream_state dropped (MongoDB reset) while the checkpoint still says batch 57: batches 0..56 would vanish
        from streaming.stream_stats import already_applied
        with self.assertRaises(RuntimeError):
            already_applied(FakeDb(), "valid_events", 57)


if __name__ == "__main__":
    unittest.main()
