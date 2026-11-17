"""Tests for merging batch user history with streamed ratings before fold-in."""
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np

from common.foldin import fold_in
from streaming.recommendation_worker import build_recommendations, claim_job


class FakeCollection:
    def __init__(self, documents=()):
        self.documents = list(documents)

    def find(self, query, projection=None):
        return [doc for doc in self.documents if doc.get("user_id") == query.get("user_id")]

    def find_one(self, query, projection=None):
        return next((doc for doc in self.documents
                     if all(doc.get(key) == value for key, value in query.items())), None)

    def replace_one(self, query, document, upsert=False):
        self.delete_one(query)
        self.documents.append(document)

    def update_one(self, query, update, upsert=False):
        document = next((doc for doc in self.documents
                         if all(doc.get(key) == value for key, value in query.items())), None)
        if document is None:
            if not upsert:
                return
            document = dict(query)
            self.documents.append(document)
        document.update(update.get("$set", {}))

    def delete_one(self, query):
        self.documents = [doc for doc in self.documents
                          if not all(doc.get(key) == value for key, value in query.items())]


class FakeDatabase:
    def __init__(self):
        now = datetime.now(timezone.utc)
        self.collections = {
            "user_profiles_v1": FakeCollection([{
                "_id": 7,
                "model_version": "v1",
                "positive_ratings": [
                    {"book_id": 1, "rating": 5},
                    {"book_id": 2, "rating": 4},
                    {"book_id": 3, "rating": 5},
                ],
                "seen_book_ids": [1, 2, 3, 4],
            }]),
            "user_ratings": FakeCollection([
                {"user_id": 7, "book_id": 2, "rating": 2, "event_time": now, "ingested_at": now},
                {"user_id": 7, "book_id": 4, "rating": 1, "event_time": now, "ingested_at": now},
                {"user_id": 7, "book_id": 5, "rating": 5, "event_time": now, "ingested_at": now},
            ]),
            "recs_live": FakeCollection(),
            "meta": FakeCollection([{
                "_id": "active",
                "model_version": "v1",
                "recs_collection": "recommendations_v1",
                "popular": [{"book_id": 2}, {"book_id": 3}],
            }]),
            "recommendations_v1": FakeCollection([{
                "_id": 7,
                "recs": [{"book_id": 6, "score": 0.9}],
                "fallback": [5, 6],
            }]),
            "recommendation_jobs": FakeCollection([{
                "_id": "job-1", "user_id": 7, "status": "processing", "claim_token": "current-token",
            }]),
        }

    def __getitem__(self, name):
        return self.collections[name]

    def __getattr__(self, name):
        try:
            return self.collections[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class RecommendationWorkerTest(unittest.TestCase):
    def test_foldin_uses_batch_profile_and_stream_rating_overrides(self):
        factors = SimpleNamespace(
            item_ids=np.asarray([1, 2, 3, 4, 5, 6], dtype=np.int32),
            item_id_set={1, 2, 3, 4, 5, 6},
            item_index={i: i - 1 for i in range(1, 7)},
            factors=np.asarray([[1, 0], [0, 1], [1, 1], [2, 0], [0, 2], [1, 2]], dtype=np.float64),
            gram=np.asarray([[7, 3], [3, 10]], dtype=np.float64),
            alpha=1.0,
            reg_param=0.1,
            min_rating=4,
            min_ratings=1,
            top_k=30,
            rank=2,
            version="v1",
        )
        db = FakeDatabase()

        written = build_recommendations(db, factors, 7, "v1", "user_profiles_v1")

        self.assertEqual(written, 1)
        live = db.recs_live.documents[0]
        self.assertEqual(live["source"], "als-foldin")
        self.assertEqual(live["model_version"], "v1")
        self.assertEqual([rec["book_id"] for rec in live["recs"]], [6])

    def test_one_known_positive_stream_rating_is_enough_for_foldin(self):
        factors = SimpleNamespace(
            item_ids=np.asarray([1, 2, 3, 4], dtype=np.int32),
            item_id_set={1, 2, 3, 4},
            item_index={i: i - 1 for i in range(1, 5)},
            factors=np.asarray([[1, 0], [0, 1], [1, 1], [2, 1]], dtype=np.float64),
            gram=np.asarray([[6, 3], [3, 3]], dtype=np.float64),
            alpha=1.0,
            reg_param=0.1,
            min_rating=4,
            min_ratings=1,
            top_k=30,
            rank=2,
            version="v1",
        )
        db = FakeDatabase()
        db.user_profiles_v1.documents[0]["positive_ratings"] = []
        db.user_profiles_v1.documents[0]["seen_book_ids"] = []
        now = datetime.now(timezone.utc)
        db.user_ratings.documents = [{
            "user_id": 7, "book_id": 1, "rating": 4,
            "event_time": now, "ingested_at": now,
        }]
        written = build_recommendations(db, factors, 7, "v1", "user_profiles_v1")
        self.assertEqual(written, 3)
        recommended_ids = [rec["book_id"] for rec in db.recs_live.documents[0]["recs"]]
        self.assertNotIn(1, recommended_ids)

    def test_foldin_waits_for_three_new_streamed_positive_books(self):
        factors = SimpleNamespace(
            item_ids=np.asarray([1, 2, 3, 4, 5, 6], dtype=np.int32),
            item_index={i: i - 1 for i in range(1, 7)},
            factors=np.asarray([[1, 0], [0, 1], [1, 1], [2, 0], [0, 2], [1, 2]], dtype=np.float64),
            gram=np.asarray([[7, 3], [3, 10]], dtype=np.float64),
            alpha=1.0,
            reg_param=0.1,
            min_rating=4,
            min_ratings=3,
            top_k=30,
            rank=2,
            version="v1",
        )
        db = FakeDatabase()
        db.user_profiles_v1.documents[0]["positive_ratings"] = [
            {"book_id": 1, "rating": 5},
            {"book_id": 2, "rating": 4},
            {"book_id": 3, "rating": 5},
        ]
        db.user_profiles_v1.documents[0]["seen_book_ids"] = []
        now = datetime.now(timezone.utc)
        db.user_ratings.documents = [
            {"user_id": 7, "book_id": 4, "rating": 5, "event_time": now, "ingested_at": now},
            {"user_id": 7, "book_id": 5, "rating": 4, "event_time": now, "ingested_at": now},
        ]

        written = build_recommendations(db, factors, 7, "v1", "user_profiles_v1")
        self.assertEqual(written, 0)
        self.assertEqual(db.recs_live.documents, [])

        db.user_ratings.documents.append(
            {"user_id": 7, "book_id": 6, "rating": 5, "event_time": now, "ingested_at": now}
        )
        written = build_recommendations(db, factors, 7, "v1", "user_profiles_v1")
        self.assertGreater(written, 0)
        self.assertEqual(db.recs_live.documents[0]["source"], "als-foldin")
        self.assertEqual(db.recs_live.documents[0]["stream_positive_ratings"], 3)

    def test_outside_top_rating_refreshes_live_and_excludes_rated_book(self):
        factors = SimpleNamespace(
            item_ids=np.asarray([1, 2, 3, 4, 5, 6], dtype=np.int32),
            item_index={i: i - 1 for i in range(1, 7)},
            factors=np.asarray([[1, 0], [0, 1], [1, 1], [2, 0], [0, 2], [-2, 1]],
                               dtype=np.float64),
            gram=np.asarray([[10, -1], [-1, 7]], dtype=np.float64),
            alpha=1.0,
            reg_param=0.1,
            min_rating=4,
            min_ratings=1,
            top_k=30,
            rank=2,
            version="v1",
        )
        db = FakeDatabase()
        db.recs_live.documents = [{
            "_id": 7,
            "model_version": "v1",
            "user_vector": fold_in([(1, 5), (2, 4), (3, 5)], factors).tolist(),
        }]
        db.user_ratings.documents = [{
            "user_id": 7, "book_id": 6, "rating": 5,
            "event_time": datetime.now(timezone.utc), "ingested_at": datetime.now(timezone.utc),
            "event_id": "outside-top-event",
        }]

        written = build_recommendations(
            db, factors, 7, "v1", "user_profiles_v1",
            trigger_event_id="outside-top-event",
        )

        live = db.recs_live.documents[0]
        self.assertGreater(written, 0)
        self.assertEqual(live["source"], "als-foldin")
        self.assertEqual(live["trigger_event_id"], "outside-top-event")
        self.assertIsNotNone(live["updated_at"])
        self.assertGreaterEqual(live["fold_in_seconds"], 0)
        self.assertGreaterEqual(live["ranking_seconds"], 0)
        self.assertGreaterEqual(live["mongo_snapshot_write_seconds"], 0)
        self.assertGreater(live["vector_delta_norm"], 0)
        self.assertNotIn(6, [rec["book_id"] for rec in live["recs"]])

    def test_in_top_rating_uses_same_foldin_path_and_refreshes_live(self):
        factors = SimpleNamespace(
            item_ids=np.asarray([1, 2, 3, 4, 5, 6], dtype=np.int32),
            item_index={i: i - 1 for i in range(1, 7)},
            factors=np.asarray([[1, 0], [0, 1], [1, 1], [2, 0], [0, 2], [-2, 1]],
                               dtype=np.float64),
            gram=np.asarray([[10, -1], [-1, 7]], dtype=np.float64),
            alpha=1.0,
            reg_param=0.1,
            min_rating=4,
            min_ratings=1,
            top_k=30,
            rank=2,
            version="v1",
        )
        db = FakeDatabase()
        before = fold_in([(1, 5), (2, 4), (3, 5)], factors)
        db.recs_live.documents = [{
            "_id": 7, "model_version": "v1", "user_vector": before.tolist(),
            "recs": [{"book_id": 6, "score": 1.0}], "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        }]
        db.user_ratings.documents = [{
            "user_id": 7, "book_id": 6, "rating": 5,
            "event_time": datetime.now(timezone.utc), "ingested_at": datetime.now(timezone.utc),
            "event_id": "in-top-event",
        }]

        written = build_recommendations(
            db, factors, 7, "v1", "user_profiles_v1", trigger_event_id="in-top-event"
        )

        live = db.recs_live.documents[0]
        self.assertGreater(written, 0)
        self.assertEqual(live["trigger_event_id"], "in-top-event")
        self.assertGreater(live["vector_delta_norm"], 0)
        self.assertGreater(live["updated_at"], datetime(2026, 1, 1, tzinfo=timezone.utc))
        self.assertNotIn(6, [rec["book_id"] for rec in live["recs"]])

    def test_low_rating_does_not_write_foldin_snapshot(self):
        factors = SimpleNamespace(
            item_ids=np.asarray([1, 2, 3, 4, 5, 6], dtype=np.int32),
            item_index={i: i - 1 for i in range(1, 7)},
            factors=np.eye(6, dtype=np.float64)[:, :2],
            gram=np.eye(2, dtype=np.float64),
            alpha=1.0,
            reg_param=0.1,
            min_rating=4,
            min_ratings=1,
            top_k=30,
            rank=2,
            version="v1",
        )
        db = FakeDatabase()
        db.user_profiles_v1.documents[0]["positive_ratings"] = []
        db.user_profiles_v1.documents[0]["seen_book_ids"] = [1]
        db.user_ratings.documents = [{
            "user_id": 7, "book_id": 1, "rating": 2,
            "event_time": datetime.now(timezone.utc), "ingested_at": datetime.now(timezone.utc),
            "event_id": "low-rating-event",
        }]

        written = build_recommendations(
            db, factors, 7, "v1", "user_profiles_v1",
            trigger_event_id="low-rating-event",
        )

        self.assertEqual(written, 0)
        self.assertEqual(db.recs_live.documents, [])


class ClaimJobTest(unittest.TestCase):
    def test_claim_job_orders_by_creation_time(self):
        class Jobs:
            def update_many(self, *_args, **_kwargs):
                pass

            def find_one_and_update(self, _query, update, **kwargs):
                self.sort = kwargs["sort"]
                self.update = update
                return {"_id": 7}

        jobs = Jobs()
        db = SimpleNamespace(recommendation_jobs=jobs)
        now = datetime.now(timezone.utc)

        result = claim_job(db, now)

        self.assertEqual(result["_id"], 7)
        self.assertEqual(jobs.sort, [("created_at", 1)])
        self.assertEqual(jobs.update["$inc"]["retry_count"], 1)
        self.assertEqual(jobs.update["$set"]["status"], "processing")
        self.assertTrue(jobs.update["$set"]["claim_token"])

    def test_stale_claim_token_cannot_publish_recommendations(self):
        factors = SimpleNamespace(
            item_ids=np.asarray([1, 2, 3], dtype=np.int32),
            item_index={1: 0, 2: 1, 3: 2},
            factors=np.asarray([[1, 0], [0, 1], [1, 1]], dtype=np.float64),
            gram=np.asarray([[2, 1], [1, 2]], dtype=np.float64),
            alpha=1.0,
            reg_param=0.1,
            min_rating=4,
            min_ratings=1,
            top_k=30,
            rank=2,
            version="v1",
        )
        db = FakeDatabase()
        db.user_ratings.documents = [{
            "user_id": 7, "book_id": 1, "rating": 5,
            "event_time": datetime.now(timezone.utc), "ingested_at": datetime.now(timezone.utc),
        }]

        written = build_recommendations(
            db, factors, 7, "v1", "user_profiles_v1", job_id="job-1", claim_token="expired-token"
        )

        self.assertEqual(written, 0)
        self.assertEqual(db.recs_live.documents, [])


if __name__ == "__main__":
    unittest.main()
