"""Unit tests for common/recs.py (used by training evaluation and by write_recs.py). Needs PySpark:
    docker compose exec spark python -m unittest tests.test_recs -v
"""
import unittest

from pyspark.sql import SparkSession

from common.recs import explode_recs, split_train_test, top_k_unseen
from common.serving import cold_start_recommendations, complete_recommendations


class RecsTest(unittest.TestCase):
    def test_complete_recommendations_shows_ten_valid_unseen_books_in_user_order(self):
        result = complete_recommendations(
            [{"book_id": 2, "score": 0.9}, {"book_id": 1, "score": 0.8},
             {"book_id": 3, "score": 0.7}],
            fallback_ids=[3, 4, 5, 6, 7],
            popular_ids=[8, 9, 10, 11, 12, 13],
            catalog_ids=range(1, 15),
            seen_ids={1, 5, 9},
            limit=10,
        )
        self.assertEqual(len(result), 10)
        self.assertEqual([book["book_id"] for book in result],
                         [2, 3, 4, 6, 7, 8, 10, 11, 12, 13])
        self.assertEqual(result[0]["score"], 0.9)
        self.assertTrue(all(book["book_id"] not in {1, 5, 9} for book in result))

    def test_recommendations_skip_books_missing_from_catalog(self):
        result = complete_recommendations(
            [{"book_id": 100, "score": 1.0}, {"book_id": 2, "score": 0.5}],
            fallback_ids=[3], popular_ids=[4], catalog_ids={1, 2, 3, 4}, seen_ids=set(), limit=3,
        )
        self.assertEqual([book["book_id"] for book in result], [2, 3, 4])

    def test_new_guest_sessions_get_distinct_stable_cold_start_shelves(self):
        catalog = [
            {"book_id": book_id, "ratings_count": 1000 - book_id, "average_rating": 4.0}
            for book_id in range(1, 101)
        ]
        first = cold_start_recommendations(catalog, {1, 2}, seed=101)
        other = cold_start_recommendations(catalog, {1, 2}, seed=202)
        repeated = cold_start_recommendations(catalog, {1, 2}, seed=101)
        first_ids = [book["book_id"] for book in first]

        self.assertEqual(len(first), 10)
        self.assertEqual(first, repeated)
        self.assertNotEqual(first_ids, [book["book_id"] for book in other])
        self.assertTrue(set(first_ids).isdisjoint({1, 2}))

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").appName("test-recs")
                     .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    def test_split_keeps_every_streamed_rating_in_train(self):
        csv = [(u, b, 4 if b % 2 else 3, False) for u in range(1, 21) for b in range(1, 21)]
        streamed = [(u, 100 + u, 5, True) for u in range(1, 21)]
        ratings = self.spark.createDataFrame(csv + streamed, "user_id int, book_id int, rating int, from_stream boolean")
        train, test, n_streamed = split_train_test(ratings, 0.2, 42)
        test_books = {r.book_id for r in test.collect()}
        test_by_user = {u: 0 for u in range(1, 21)}
        for row in test.collect():
            test_by_user[row.user_id] += 1
        self.assertEqual(n_streamed, 20)
        self.assertTrue(all(b < 100 for b in test_books))  # no streamed rating held out
        self.assertEqual(train.filter("book_id >= 100").count(), 20)
        self.assertEqual(train.count() + test.count(), 220)  # 200 positive CSV ratings + 20 streamed
        self.assertEqual(set(test_by_user.values()), {2})
        self.assertEqual(train.filter("rating < 4").count(), 0)

    def test_split_works_on_parquet_without_from_stream(self):
        ratings = self.spark.createDataFrame([(1, b, 4) for b in range(50)], "user_id int, book_id int, rating int")
        train, test, n_streamed = split_train_test(ratings, 0.2, 42)
        self.assertEqual((n_streamed, train.count() + test.count()), (0, 50))

    def test_one_rating_user_stays_in_train_and_low_ratings_are_ignored(self):
        ratings = self.spark.createDataFrame(
            [(1, 10, 5, False), (2, 20, 3, False)],
            "user_id int, book_id int, rating int, from_stream boolean")
        train, test, _ = split_train_test(ratings, 0.2, 42)
        self.assertEqual([(r.user_id, r.book_id) for r in train.collect()], [(1, 10)])
        self.assertEqual(test.count(), 0)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_explode_recs_flattens_the_als_output(self):
        raw = self.spark.createDataFrame(
            [(1, [(10, 0.9), (11, 0.5)])],
            "user_id int, recommendations array<struct<book_id:int,rating:float>>")
        rows = sorted((r.user_id, r.book_id) for r in explode_recs(raw).collect())
        self.assertEqual(rows, [(1, 10), (1, 11)])

    def test_top_k_unseen_removes_read_books_and_keeps_the_best_k(self):
        candidates = self.spark.createDataFrame(
            [(1, 10, 0.9), (1, 11, 0.8), (1, 12, 0.7), (1, 13, 0.6), (2, 10, 0.5), (2, 14, 0.4)],
            "user_id int, book_id int, score double")
        seen = self.spark.createDataFrame([(1, 11), (2, 14)], "user_id int, book_id int")
        result = {(r.user_id, r.rank): r.book_id for r in top_k_unseen(candidates, seen, 2).collect()}
        # user 1 already read 11, so the top-2 is 10 then 12; user 2 already read 14, only 10 is left
        self.assertEqual(result, {(1, 1): 10, (1, 2): 12, (2, 1): 10})


if __name__ == "__main__":
    unittest.main()
