"""Replay held-out positive ratings and compare batch recommendations with user fold-in.

Run after batch/train_als.py --implicit. This is a retrospective offline simulation: it measures ranking
changes without claiming real-user or production latency.
"""
import argparse
import random
import time

import numpy as np
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.foldin import fold_in, load_model, recommend
from common.recs import explode_recs, split_train_test, top_k_unseen
from common.settings import (
    ALS_SEED, ALS_TEST_RATIO, FOLD_IN_MIN_RATINGS, FOLD_IN_TOP_K, PARQUET_DIR, current_model, get_spark,
    save_report,
)


def metrics(recommendations, relevant, k):
    values = []
    for user_id, target in relevant.items():
        picks = recommendations.get(user_id, [])[:k]
        hits = len(set(picks) & target)
        values.append((hits / k, hits / len(target)))
    if not values:
        return {"precision@10": None, "recall@10": None, "users": 0}
    return {f"precision@{k}": round(sum(p for p, _ in values) / len(values), 4),
            f"recall@{k}": round(sum(r for _, r in values) / len(values), 4), "users": len(values)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--eval-users", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=None,
                        help="split seed; defaults to the seed saved in the model manifest (ALS_SEED)")
    parser.add_argument("--candidates", type=int, default=200)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--vector-check-users", type=int, default=20)
    parser.add_argument("--benchmark-users", type=int, default=1000)
    args = parser.parse_args()

    model_path, manifest = current_model("als_implicit")
    if not manifest.get("implicitPrefs", manifest.get("implicit")):
        raise RuntimeError("The active ALS artifact is not an implicit-feedback model")
    fold_model = load_model(model_path)
    # Rebuild exactly the train/test split the model was trained on, otherwise held-out targets could
    # have been seen in training.
    if args.seed is None:
        args.seed = int(manifest.get("seed", ALS_SEED))
    test_ratio = float(manifest.get("test_ratio", ALS_TEST_RATIO))

    spark = get_spark("bookpulse-foldin-eval")
    model = ALSModel.load(str(model_path / "spark_model"))
    started = time.time()
    ratings = spark.read.parquet(str(PARQUET_DIR / "ratings"))
    train, test, _ = split_train_test(ratings, test_ratio, args.seed, int(manifest["min_rating"]))
    # Materialise the split once. The split orders rows by rand() inside a per-user window, so a later
    # filter on user_id pushed below the window would re-draw rand() and give a different split.
    train, test = train.cache(), test.cache()
    train.count(), test.count()

    test_window = Window.partitionBy("user_id")
    order_window = Window.partitionBy("user_id").orderBy(F.rand(args.seed + 1), F.asc("book_id"))
    ranked_test = (test.withColumn("_n", F.count("*").over(test_window))
                   .withColumn("_rank", F.row_number().over(order_window)))
    target_count = F.when(F.col("_n") >= 2,
                          F.greatest(F.lit(1), F.least(F.col("_n") - 1, F.floor(F.col("_n") / 2).cast("int"))))
    replay = (ranked_test.filter(F.col("_n") >= 2)
              .filter(F.col("_rank") <= F.col("_n") - target_count)
              .select("user_id", "book_id", "rating"))
    target = (ranked_test.filter(F.col("_n") >= 2)
              .filter(F.col("_rank") > F.col("_n") - target_count)
              .select("user_id", "book_id"))
    eval_users = (target.select("user_id").distinct().orderBy(F.rand(args.seed + 2))
                  .limit(args.eval_users).cache())
    replay = replay.join(eval_users, "user_id").cache()
    target = target.join(eval_users, "user_id").cache()
    base_seen = (ratings.join(test.select("user_id", "book_id"), ["user_id", "book_id"], "left_anti")
                 .join(eval_users, "user_id"))
    # "Before" = batch list with the replayed books hidden (what the app shows without fold-in).
    hidden_after_replay = base_seen.select("user_id", "book_id").unionByName(replay.select("user_id", "book_id"))
    before_raw = model.recommendForUserSubset(eval_users, args.candidates)
    before_rows = top_k_unseen(explode_recs(before_raw), hidden_after_replay, args.top_n).collect()
    before = {}
    for row in before_rows:
        before.setdefault(row["user_id"], []).append(row["book_id"])
    relevant = {}
    for row in target.groupBy("user_id").agg(F.collect_set("book_id").alias("books")).collect():
        relevant[row["user_id"]] = set(row["books"])

    seen_by_user = {row["user_id"]: set(row["books"]) for row in
                    base_seen.groupBy("user_id").agg(F.collect_set("book_id").alias("books")).collect()}
    replay_by_user = {row["user_id"]: [(r["book_id"], r["rating"]) for r in row["ratings"]]
                      for row in replay.groupBy("user_id").agg(
                          F.collect_list(F.struct("book_id", "rating")).alias("ratings")).collect()}
    # Training positives of each user: the worker folds in history + new ratings, never new ratings alone.
    history_by_user = {row["user_id"]: [(r["book_id"], r["rating"]) for r in row["ratings"]]
                       for row in train.join(eval_users, "user_id").groupBy("user_id").agg(
                           F.collect_list(F.struct("book_id", "rating")).alias("ratings")).collect()}
    batch_candidates = {}
    for row in explode_recs(before_raw).collect():
        batch_candidates.setdefault(int(row["user_id"]), []).append(
            (int(row["book_id"]), float(row["score"]))
        )
    for candidates in batch_candidates.values():
        candidates.sort(key=lambda item: (-item[1], item[0]))

    after = {}
    recommendation_seconds = []
    for user_id, values in replay_by_user.items():
        started_user = time.perf_counter()
        after[user_id] = [
            book_id for book_id, _ in recommend(
                history_by_user.get(user_id, []) + values, seen_by_user.get(user_id, set()),
                fold_model, args.top_n
            )
        ]
        recommendation_seconds.append(time.perf_counter() - started_user)

    jaccards = []
    for user_id in relevant:
        left, right = set(before.get(user_id, [])), set(after.get(user_id, []))
        union = left | right
        jaccards.append(len(left & right) / len(union) if union else 1.0)

    # So sánh 20 user ngẫu nhiên với đúng các positive rating đã dùng để train.
    vector_users = (train.groupBy("user_id").agg(F.count("*").alias("n"))
                    .filter(F.col("n") >= FOLD_IN_MIN_RATINGS)
                    .orderBy(F.rand(args.seed + 3)).limit(args.vector_check_users)
                    .select("user_id").collect())
    vector_user_ids = [int(row["user_id"]) for row in vector_users]
    ratings_by_user = {}
    if vector_user_ids:
        check_rows = (train.filter(F.col("user_id").isin(vector_user_ids))
                      .select("user_id", "book_id", "rating").collect())
        for row in check_rows:
            ratings_by_user.setdefault(int(row["user_id"]), []).append(
                (int(row["book_id"]), float(row["rating"]))
            )
    saved_by_user = {
        int(row["id"]): np.asarray(row["features"], dtype=np.float64)
        for row in model.userFactors.filter(F.col("id").isin(vector_user_ids)).collect()
    } if vector_user_ids else {}
    vector_checks = []
    for user_id in vector_user_ids:
        solved = fold_in(ratings_by_user.get(user_id, []), fold_model)
        saved = saved_by_user.get(user_id)
        if solved is None or saved is None:
            continue
        denominator = float(np.linalg.norm(solved) * np.linalg.norm(saved))
        cosine = float(np.dot(solved, saved) / denominator) if denominator else 0.0
        vector_checks.append({
            "user_id": user_id,
            "cosine_similarity": cosine,
            "max_absolute_error": float(np.max(np.abs(solved - saved))),
        })

    # Mô phỏng user có lịch sử train rồi chấm thêm đúng N rating dương mới (chọn ngẫu nhiên có seed);
    # rating thấp chỉ được dùng để ẩn sách.
    rating_count_groups = []
    train_positive_counts = {
        int(row["user_id"]): int(row["n"])
        for row in train.groupBy("user_id").agg(F.count("*").alias("n")).collect()
    }
    for rating_count in (1, 2, 3, 5, 10):
        eligible = {}
        group_before = {}
        group_after = {}
        group_jaccards = []
        for user_id, values in replay_by_user.items():
            positives = sorted(
                ((int(book_id), float(rating)) for book_id, rating in values
                 if float(rating) >= int(manifest["min_rating"])),
                key=lambda pair: pair[0],
            )
            random.Random(args.seed * 1_000_003 + int(user_id)).shuffle(positives)
            if len(positives) < rating_count:
                continue
            selected_positive = positives[:rating_count]
            low_rated = [(int(book_id), float(rating)) for book_id, rating in values
                         if float(rating) < int(manifest["min_rating"])]
            current_seen = set(seen_by_user.get(user_id, set()))
            current_seen.update(book_id for book_id, _ in selected_positive)
            current_seen.update(book_id for book_id, _ in low_rated)
            eligible[user_id] = (selected_positive, low_rated, current_seen)
            group_before[user_id] = [
                book_id for book_id, _ in batch_candidates.get(user_id, [])
                if book_id not in current_seen
            ][:args.top_n]
            group_after[user_id] = [
                book_id for book_id, _ in recommend(
                    history_by_user.get(user_id, []) + selected_positive + low_rated, current_seen,
                    fold_model, args.top_n
                )
            ]
            left, right = set(group_before[user_id]), set(group_after[user_id])
            union = left | right
            group_jaccards.append(len(left & right) / len(union) if union else 1.0)

        group_relevant = {user_id: relevant[user_id] for user_id in eligible if user_id in relevant}
        group_before = {user_id: group_before[user_id] for user_id in group_relevant}
        group_after = {user_id: group_after[user_id] for user_id in group_relevant}

        exact_ids = [user_id for user_id, count in train_positive_counts.items()
                     if count == rating_count]
        if exact_ids:
            exact_ids = exact_ids[:args.vector_check_users]
            exact_ratings = {}
            for row in (train.filter(F.col("user_id").isin(exact_ids))
                        .select("user_id", "book_id", "rating").collect()):
                exact_ratings.setdefault(int(row["user_id"]), []).append(
                    (int(row["book_id"]), float(row["rating"]))
                )
            exact_factors = {
                int(row["id"]): np.asarray(row["features"], dtype=np.float64)
                for row in model.userFactors.filter(F.col("id").isin(exact_ids)).collect()
            }
            exact_checks = []
            for user_id in exact_ids:
                solved = fold_in(exact_ratings.get(user_id, []), fold_model)
                saved = exact_factors.get(user_id)
                if solved is None or saved is None:
                    continue
                denominator = float(np.linalg.norm(solved) * np.linalg.norm(saved))
                exact_checks.append({
                    "cosine_similarity": float(np.dot(solved, saved) / denominator)
                    if denominator else 0.0,
                    "max_absolute_error": float(np.max(np.abs(solved - saved))),
                })
        else:
            exact_checks = []

        rating_count_groups.append({
            "positive_ratings": rating_count,
            "eligible_replay_users": len(group_relevant),
            "before": metrics(group_before, group_relevant, args.top_n),
            "after_foldin": metrics(group_after, group_relevant, args.top_n),
            "top_k_jaccard_before_after": (
                float(np.mean(group_jaccards)) if group_jaccards else None
            ),
            "trained_user_vector_checks": {
                "users_available_with_exact_rating_count": len(exact_ids),
                "users_measured": len(exact_checks),
                "mean_cosine_similarity": (
                    float(np.mean([item["cosine_similarity"] for item in exact_checks]))
                    if exact_checks else None
                ),
                "max_absolute_error": (
                    max(item["max_absolute_error"] for item in exact_checks)
                    if exact_checks else None
                ),
            },
        })

    # Đo thực tế cho tối đa 1.000 user phân biệt; số đo phản ánh đúng số đã chạy.
    benchmark_pairs = list(replay_by_user.items())[:max(0, args.benchmark_users)]
    benchmark_seconds = []
    benchmark_started = time.perf_counter()
    for user_id, values in benchmark_pairs:
        started_user = time.perf_counter()
        recommend(history_by_user.get(user_id, []) + values, seen_by_user.get(user_id, set()), fold_model,
                  FOLD_IN_TOP_K)
        benchmark_seconds.append(time.perf_counter() - started_user)
    benchmark_total = time.perf_counter() - benchmark_started

    save_report("foldin_eval", {
        "model_version": manifest["model_version"],
        "min_rating": manifest["min_rating"],
        "split": {"test_ratio": test_ratio, "seed": args.seed},
        "comparison": "before = batch list with replayed books hidden; after = fold-in of train history + replay",
        "replay_ratings": replay.count(),
        "target_ratings": target.count(),
        "users_evaluated": len(relevant),
        "before": metrics(before, relevant, args.top_n),
        "after_foldin": metrics(after, relevant, args.top_n),
        "rating_count_groups": rating_count_groups,
        "top10_jaccard_before_after": round(sum(jaccards) / len(jaccards), 4) if jaccards else None,
        "trained_user_vector_checks": {
            "requested": args.vector_check_users,
            "measured": len(vector_checks),
            "users": vector_checks,
            "mean_cosine_similarity": (
                float(np.mean([row["cosine_similarity"] for row in vector_checks])
                      ) if vector_checks else None
            ),
            "max_absolute_error": (
                max(row["max_absolute_error"] for row in vector_checks) if vector_checks else None
            ),
        },
        "recommendation_latency_seconds": {
            "users_measured": len(recommendation_seconds),
            "single_user": recommendation_seconds[0] if recommendation_seconds else None,
            "mean_per_user": float(np.mean(recommendation_seconds)) if recommendation_seconds else None,
            "p95_per_user": (
                float(np.percentile(recommendation_seconds, 95)) if recommendation_seconds else None
            ),
        },
        "sequential_benchmark": {
            "users_requested": args.benchmark_users,
            "users_measured": len(benchmark_seconds),
            "total_seconds": benchmark_total,
            "mean_per_user": float(np.mean(benchmark_seconds)) if benchmark_seconds else None,
            "p95_per_user": (
                float(np.percentile(benchmark_seconds, 95)) if benchmark_seconds else None
            ),
        },
        "offline_simulation": True,
        "total_seconds": round(time.time() - started, 1),
    })
    spark.stop()


if __name__ == "__main__":
    main()
