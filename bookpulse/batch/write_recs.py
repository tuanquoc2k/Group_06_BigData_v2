"""Batch step 3: top-N unseen books per user -> MongoDB, then switch the app to the new version.

Recommendations and user history profiles go to new versioned collections. Only when every document is
written does the meta document "active" point to them, so the app and worker never read partial data.
Book titles are upserted before the switch. Existing versions are retained for rollback and inspection.

Each user document also carries "fallback": the most-rated books that user has not rated, which the
app uses to refill the top-10 once the user has read through the stored recommendations.
"""
import argparse
import time
from datetime import datetime, timezone

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F
from pymongo import UpdateOne

from common.recs import explode_recs, top_k_unseen
from common.settings import (ALS_MIN_RATING, MODEL_DIR, MONGO_DB, MONGO_URI, PARQUET_DIR,
                             current_model, get_db, get_spark, save_report)

def write_partition_factory(collection, version, created_at):
    def write_partition(rows):
        from pymongo import MongoClient, ReplaceOne

        coll = MongoClient(MONGO_URI)[MONGO_DB][collection]
        ops = []
        for row in rows:
            recs = [{"rank": r["rank"], "book_id": r["book_id"], "score": round(float(r["score"]), 4)}
                    for r in row["recs"] or []]
            fallback = [r["book_id"] for r in row["fallback"] or []]
            ops.append(ReplaceOne({"_id": row["user_id"]},
                                  {"_id": row["user_id"], "recs": recs, "fallback": fallback,
                                   "model_version": version, "created_at": created_at}, upsert=True))
            if len(ops) == 1000:
                coll.bulk_write(ops, ordered=False)
                ops = []
        if ops:
            coll.bulk_write(ops, ordered=False)

    return write_partition


def profile_partition_factory(collection, version):
    def write_partition(rows):
        from pymongo import MongoClient, ReplaceOne

        coll = MongoClient(MONGO_URI)[MONGO_DB][collection]
        ops = []
        for row in rows:
            ops.append(ReplaceOne(
                {"_id": row["user_id"]},
                {"_id": row["user_id"], "model_version": version,
                 "positive_ratings": [{"book_id": r["book_id"], "rating": r["rating"]}
                                      for r in row["positive_ratings"] or []],
                 "seen_book_ids": row["seen_book_ids"]},
                upsert=True))
            if len(ops) == 1000:
                coll.bulk_write(ops, ordered=False)
                ops = []
        if ops:
            coll.bulk_write(ops, ordered=False)

    return write_partition


def load_books(spark, db, path):
    """Upsert basic book fields without overwriting stored genres or other metadata."""
    books = (spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(path)
             .select(F.col("book_id").cast("int"), "title", "authors").dropna(subset=["book_id"]))
    docs = [{"_id": r["book_id"], "title": r["title"], "authors": r["authors"]} for r in books.collect()]
    if docs:
        db.books.bulk_write([
            UpdateOne({"_id": doc["_id"]},
                      {"$set": {"title": doc["title"], "authors": doc["authors"]}},
                      upsert=True)
            for doc in docs
        ], ordered=False)
    return len(docs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=30, help="stored per user; the app shows the first 10 unread")
    parser.add_argument("--candidates", type=int, default=200)
    parser.add_argument("--books", default=None, help="books.csv with book_id,title,authors (optional)")
    parser.add_argument("--fallback-n", type=int, default=10,
                        help="popular books the user has not rated, stored per user to refill the top-10")
    parser.add_argument("--fallback-pool", type=int, default=200, help="most-rated books to pick the fallback from")
    parser.add_argument("--model", default="als_implicit", choices=["als_implicit", "als"],
                        help="serving model in models/: als_implicit (chosen on Precision@10, see "
                             "docs/chon-mo-hinh.md) or als (explicit, predicts stars)")
    args = parser.parse_args()

    spark = get_spark("bookpulse-write-recs")
    db = get_db()
    started = time.time()
    created_at = datetime.now(timezone.utc)
    version = created_at.strftime("%Y%m%d_%H%M%S_%f")
    collection = f"recommendations_{version}"

    ratings = spark.read.parquet(str(PARQUET_DIR / "ratings"))
    seen = ratings.select("user_id", "book_id")
    model_path, manifest = current_model(args.model)
    if not manifest:
        model_path = MODEL_DIR / args.model
    model = ALSModel.load(str(model_path / "spark_model" if manifest else model_path))
    model_type = "implicit" if args.model == "als_implicit" else "explicit"
    model_version = manifest.get("model_version", version)
    min_rating = manifest.get("min_rating", ALS_MIN_RATING)
    positive = ratings.filter(F.col("rating") >= min_rating)
    recs = top_k_unseen(explode_recs(model.recommendForAllUsers(args.candidates)), seen, args.top_n)
    per_user = recs.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("rank", "book_id", "score"))).alias("recs"))

    # Per-user refill list: most-rated books this user has NOT rated. When the user reads through the
    # stored recs, the app tops the list up from here instead of showing fewer than 10 books
    # (a global popular list could repeat books the user already read).
    pool = (positive.groupBy("book_id").agg(F.count("*").alias("score"))
            .orderBy(F.desc("score"), "book_id").limit(args.fallback_pool))
    fallback = (top_k_unseen(seen.select("user_id").distinct().crossJoin(F.broadcast(pool)), seen,
                             args.fallback_n)
                .groupBy("user_id")
                .agg(F.sort_array(F.collect_list(F.struct("rank", "book_id"))).alias("fallback")))
    per_user = per_user.join(fallback, "user_id", "left").cache()
    expected_users = per_user.count()
    profile_collection = f"user_profiles_{model_version}_{version}"
    positive_profiles = (positive.groupBy("user_id")
                         .agg(F.collect_list(F.struct("book_id", "rating")).alias("positive_ratings")))
    profiles = (seen.groupBy("user_id")
                .agg(F.collect_set("book_id").alias("seen_book_ids"))
                .join(positive_profiles, "user_id", "left")
                .withColumn("model_version", F.lit(model_version)).cache())
    expected_profiles = profiles.count()
    try:
        per_user.foreachPartition(write_partition_factory(collection, model_version, created_at))
        users_written = db[collection].count_documents({})
        profiles.foreachPartition(profile_partition_factory(profile_collection, model_version))
        profiles_written = db[profile_collection].count_documents({})
        # Switch only after both recommendation and rating-profile collections are complete.
        if expected_users == 0 or users_written != expected_users:
            raise RuntimeError(f"{collection}: wrote {users_written} of {expected_users} users")
        if expected_profiles == 0 or profiles_written != expected_profiles:
            raise RuntimeError(f"{profile_collection}: wrote {profiles_written} of {expected_profiles} users")
    except Exception:
        # Keep the incomplete collections for inspection; the active pointer has not changed yet.
        raise

    # Fallback list for users the model has never seen (cold start)
    popular = (positive.groupBy("book_id")
               .agg(F.count("*").alias("n_ratings"), F.avg("rating").alias("avg_rating"))
               .orderBy(F.desc("n_ratings")).limit(100).collect())
    popular_docs = [{"book_id": r["book_id"], "n_ratings": r["n_ratings"],
                     "avg_rating": round(r["avg_rating"], 3)} for r in popular]

    # Book titles first (upsert), so they are there when the app switches to the new version
    books_loaded = load_books(spark, db, args.books) if args.books else None

    # Atomic switch: the app reads whatever collection "active" points to. The version that was active
    # until now is kept as "previous_collection" for a manual rollback (set recs_collection back to it).
    previous_meta = db.meta.find_one({"_id": "active"}) or {}
    previous = previous_meta.get("recs_collection")
    previous_profiles = previous_meta.get("user_profiles_collection")
    # Keep historical live documents, but they are never served for a different active model.
    db.meta.replace_one({"_id": "active"}, {"_id": "active", "recs_collection": collection,
                                             "model_version": model_version, "model_type": model_type,
                                             "foldin_available": bool(
                                                 manifest.get("implicit", False)
                                                 and manifest.get("nonnegative") is False
                                                 and (
                                                     all((model_path / name).is_file() for name in
                                                         ("item_ids.npy", "item_factors.npy", "YtY.npy"))
                                                     or (model_path / "item_factors.npz").is_file()
                                                 )),
                                             "user_profiles_collection": profile_collection,
                                             "previous_collection": previous,
                                             "previous_profiles_collection": previous_profiles,
                                             "popular": popular_docs,
                                             "switched_at": datetime.now(timezone.utc)}, upsert=True)
    db.recs_live.update_many(
        {"model_version": {"$ne": model_version}},
        {"$set": {"stale": True, "stale_at": datetime.now(timezone.utc)}},
    )
    profiles.unpersist()
    save_report("write_recs", {
        "model_version": model_version,
        "serving_version": version,
        "model": args.model,
        "min_rating": min_rating,
        "collection": collection,
        "users_written": users_written,
        "profile_collection": profile_collection,
        "profile_users_expected": expected_profiles,
        "profile_users_written": profiles_written,
        "top_n": args.top_n,
        "fallback_n": args.fallback_n,
        "users_with_full_fallback": db[collection].count_documents(
            {f"fallback.{args.fallback_n - 1}": {"$exists": True}}),
        "books_loaded": books_loaded,
        "expected_users": expected_users,
        "previous_collection": previous,
        "old_versions_retained": True,
        "seconds": round(time.time() - started, 1),
    })
    spark.stop()


if __name__ == "__main__":
    main()
