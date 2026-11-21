"""Recommendation helpers used by training (evaluation), the job that writes to MongoDB, and the scripts that
read the served list back (visible_top10: the same rule as the app page)."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.settings import ALS_MIN_RATING


def explode_recs(raw):
    """ALS recommendForUserSubset/AllUsers output -> rows of (user_id, book_id, score)."""
    return raw.select("user_id", F.explode("recommendations").alias("r")).select(
        "user_id", F.col("r.book_id").alias("book_id"), F.col("r.rating").alias("score")
    )


def top_k_unseen(candidates, seen, k):
    """Keep the k best-scored candidates per user after removing books the user already rated."""
    w = Window.partitionBy("user_id").orderBy(F.desc("score"), F.asc("book_id"))
    return (
        candidates.join(seen, ["user_id", "book_id"], "left_anti")
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= k)
    )


def split_train_test(ratings, test_ratio, seed, min_rating=ALS_MIN_RATING):
    """Train/test split used by train_als.py. Returns (train, test, number of streamed ratings in train).

    Only positive ratings are training/evaluation signals. CSV rows are randomly held out per user;
    streamed ratings are newest and always stay in train. Parquet from an older clean.py has no from_stream.

    Reproducible only over the full ratings table: cache the result before filtering by user_id, because
    Spark pushes such a filter below the window and rand() then draws different values.
    """
    if "from_stream" not in ratings.columns:
        ratings = ratings.withColumn("from_stream", F.lit(False))
    positive = ratings.filter(F.col("rating") >= min_rating)
    csv_ratings = positive.filter(~F.col("from_stream")).select("user_id", "book_id", "rating")
    streamed = positive.filter(F.col("from_stream")).select("user_id", "book_id", "rating")
    user_window = Window.partitionBy("user_id")
    order_window = Window.partitionBy("user_id").orderBy(F.rand(seed), F.asc("book_id"))
    ranked = (csv_ratings.withColumn("_user_count", F.count("*").over(user_window))
              .withColumn("_rank", F.row_number().over(order_window))
              .withColumn("_test_count", F.when(F.col("_user_count") > 1,
                                                F.greatest(F.lit(1), F.least(
                                                    F.col("_user_count") - 1,
                                                    F.floor(F.col("_user_count") * test_ratio).cast("int"))))
                          .otherwise(F.lit(0))))
    train_csv = ranked.filter(F.col("_rank") <= F.col("_user_count") - F.col("_test_count")).select(
        "user_id", "book_id", "rating")
    test = ranked.filter(F.col("_rank") > F.col("_user_count") - F.col("_test_count")).select(
        "user_id", "book_id", "rating")
    return train_csv.unionByName(streamed), test, streamed.count()


def visible_top10(db, user_id, k=10):
    """The list the app shows for a user: stored recommendations of the active version minus the books in user_seen
    (rated through the stream since the last reset), first k, with titles. Needs a pymongo database, not Spark."""
    meta = db.meta.find_one({"_id": "active"})
    if meta is None:
        return {"model_version": None, "recs_collection": None, "top10": []}
    seen = {d["book_id"] for d in db.user_seen.find({"user_id": user_id}, {"book_id": 1})}
    live = db.recs_live.find_one({"_id": user_id, "model_version": meta["model_version"]})
    doc = db[meta["recs_collection"]].find_one({"_id": user_id})
    source = "als-foldin" if live else "batch"
    recs = [r for r in ((live or doc or {}).get("recs", [])) if r["book_id"] not in seen][:k]
    titles = {b["_id"]: b.get("title") for b in db.books.find({"_id": {"$in": [r["book_id"] for r in recs]}})}
    return {"model_version": meta["model_version"], "recs_collection": meta["recs_collection"],
            "source": source,
            "top10": [{"rank": i + 1, "book_id": r["book_id"], "score": r.get("score"),
                       "title": titles.get(r["book_id"])} for i, r in enumerate(recs)]}
