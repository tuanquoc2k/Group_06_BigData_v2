"""Unit tests for implicit-ALS fold-in and recommendation ranking."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from common.foldin import fold_in, fold_in_user, load_model, recommend
from common.settings import ALS_MIN_RATING


def small_model():
    item_ids = np.asarray([10, 11, 12, 13, 14], dtype=np.int64)
    factors = np.asarray([
        [1.0, 0.0],
        [0.0, 1.0],
        [1.0, 1.0],
        [-1.0, 0.5],
        [0.2, -1.0],
    ], dtype=np.float64)
    return SimpleNamespace(
        item_ids=item_ids,
        factors=factors,
        gram=factors.T @ factors,
        item_index={int(book_id): index for index, book_id in enumerate(item_ids)},
        version="test",
        alpha=1.0,
        reg_param=0.1,
        rank=2,
        min_rating=4,
        min_ratings=3,
        top_k=30,
    )


class FoldInTest(unittest.TestCase):
    def test_solves_implicit_confidence_equation_and_ignores_unknown_items(self):
        item_ids = np.asarray([10, 11, 12], dtype=np.int32)
        factors = np.asarray([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]], dtype=np.float64)
        ratings = [(10, 5), (11, 4), (99, 5)]
        alpha, reg_param = 2.0, 0.1
        gram = factors.T @ factors
        observed = factors[:2]
        confidence_delta = np.asarray([alpha * 5, alpha * 4], dtype=np.float64)
        confidence = confidence_delta + 1
        lhs = gram + (observed.T * confidence_delta) @ observed
        lhs.flat[::lhs.shape[0] + 1] += reg_param * len(observed)
        expected = np.linalg.solve(lhs, observed.T @ confidence)
        actual = fold_in_user(item_ids, factors, ratings, alpha, reg_param, gram)
        np.testing.assert_allclose(actual, expected)

    def test_returns_none_without_known_positive_ratings(self):
        result = fold_in_user(np.asarray([1]), np.asarray([[1.0, 0.0]]), [(2, 5)], 1.0, 0.1)
        self.assertIsNone(result)

    def test_recommendations_exclude_all_rated_books(self):
        model = small_model()
        ratings = [(10, 5), (11, 4), (12, 4)]
        results = recommend(ratings, {13}, model, k=3)
        ids = [book_id for book_id, _ in results]
        self.assertEqual(len(ids), 1)
        self.assertTrue(set(ids).isdisjoint({10, 11, 12, 13}))

    def test_adding_a_positive_rating_changes_the_user_vector(self):
        model = small_model()
        initial = fold_in([(10, 5), (11, 4), (12, 4)], model)
        updated = fold_in([(10, 5), (11, 4), (12, 4), (13, 5)], model)
        self.assertFalse(np.allclose(initial, updated))

    def test_low_rating_does_not_change_vector_but_is_excluded(self):
        model = small_model()
        positives = [(10, 5), (11, 4), (12, 4)]
        with_low = positives + [(13, 2)]
        np.testing.assert_allclose(fold_in(positives, model), fold_in(with_low, model))
        results = recommend(with_low, {10, 11, 12}, model, k=30)
        self.assertNotIn(13, [book_id for book_id, _ in results])

    def test_below_minimum_positive_ratings_returns_none(self):
        model = small_model()
        self.assertIsNone(fold_in([(10, 5), (11, 4), (12, 2)], model))

    def test_recommendations_are_idempotent(self):
        model = small_model()
        ratings = [(10, 5), (11, 4), (12, 4)]
        first = recommend(ratings, {14}, model, k=2)
        second = recommend(ratings, {14}, model, k=2)
        self.assertEqual(first, second)

    def test_fold_in_uses_the_complete_item_gramian(self):
        model = small_model()
        ratings = [(10, 5), (11, 4), (12, 4)]
        actual = fold_in(ratings, model)
        expected = fold_in_user(
            model.item_ids, model.factors, ratings, model.alpha, model.reg_param,
            model.factors.T @ model.factors, model.min_rating, model.item_index,
        )
        np.testing.assert_allclose(actual, expected)

    def test_load_model_reads_npy_artifacts_and_builds_book_index(self):
        source = small_model()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "manifest.json").write_text(json.dumps({
                "model_version": "test-version",
                "rank": source.rank,
                "alpha": source.alpha,
                "regParam": source.reg_param,
                "implicitPrefs": True,
                "rating_threshold": ALS_MIN_RATING,
            }), encoding="utf-8")
            np.save(path / "item_ids.npy", source.item_ids)
            saved_factors = source.factors.astype(np.float32)
            np.save(path / "item_factors.npy", saved_factors)
            factors64 = saved_factors.astype(np.float64)
            np.save(path / "YtY.npy", factors64.T @ factors64)

            model = load_model(path, active_model_version="test-version")

        self.assertEqual(model.version, "test-version")
        self.assertEqual(model.item_index[12], 2)
        self.assertEqual(model.rank, 2)
        self.assertEqual(model.min_rating, ALS_MIN_RATING)

    def test_matches_spark_implicit_als_user_factor(self):
        from pyspark.ml.recommendation import ALS
        from pyspark.sql import SparkSession

        spark = (SparkSession.builder.master("local[2]").appName("foldin-test")
                 .config("spark.ui.enabled", "false").getOrCreate())
        try:
            rows = [(u, i, 4 + ((u + i) % 2)) for u in range(1, 7) for i in range(1, 9)
                    if (u * i) % 5 != 0]
            ratings = spark.createDataFrame(rows, ["user_id", "book_id", "rating"])
            model = ALS(userCol="user_id", itemCol="book_id", ratingCol="rating",
                        rank=3, maxIter=6, regParam=0.1, alpha=1.0, implicitPrefs=True,
                        nonnegative=False, seed=42).fit(ratings)
            item_rows = model.itemFactors.orderBy("id").collect()
            item_ids = np.asarray([row["id"] for row in item_rows], dtype=np.int32)
            item_factors = np.asarray([row["features"] for row in item_rows], dtype=np.float64)
            user_ratings = [(row["book_id"], row["rating"]) for row in
                            ratings.filter("user_id = 1").select("book_id", "rating").collect()]
            actual = fold_in_user(item_ids, item_factors, user_ratings, 1.0, 0.1)
            expected = np.asarray(
                model.userFactors.filter("id = 1").first()["features"], dtype=np.float64
            )
            np.testing.assert_allclose(actual, expected, rtol=1e-4, atol=1e-5)
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
