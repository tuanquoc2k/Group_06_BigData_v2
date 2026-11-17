"""Batch step 2: baselines + ALS (tuned with TrainValidationSplit) + evaluation on a held-out test set.

RMSE is compared with the global-mean and item-mean baselines; Precision@10 / Recall@10 with the
most-popular baseline. A book is "relevant" when the user rated it 4 or 5 in the test set, and books
the user already rated in training are never recommended.

--implicit trains implicit-feedback ALS instead (ratings used as confidence weights). It optimises
ranking rather than star error, so it is tuned and compared on Precision@10 / Recall@10 only: every
rank x regParam x alpha combination is fitted on part of train and scored on a validation slice of
train (never on the test set); the best one is refitted on all of train, evaluated once on test and
saved to models/als_implicit. This is the serving model (see docs/chon-mo-hinh.md).

Ranking results also report how many distinct books the top-10 lists cover, to show whether a
model only repeats the same popular books.
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

import numpy as np
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import functions as F

from common.recs import explode_recs, split_train_test, top_k_unseen
from common.settings import (
    ALS_ALPHAS, ALS_EVAL_USERS, ALS_MAX_ITER, ALS_MIN_RATING, ALS_RANKS, ALS_REGS, ALS_SEED,
    ALS_TEST_RATIO, CHECKPOINT_DIR, MODEL_DIR, PARQUET_DIR, get_spark, save_report,
)

K = 10


def ranking_metrics(recs, relevant, k):
    """Average Precision@k and Recall@k over users that have at least one relevant test book."""
    n_rel = relevant.groupBy("user_id").agg(F.count("*").alias("n_rel"))
    hits = recs.join(relevant, ["user_id", "book_id"]).groupBy("user_id").agg(F.count("*").alias("hits"))
    row = (
        n_rel.join(hits, "user_id", "left").fillna(0, subset=["hits"])
        .agg(
            F.avg(F.col("hits") / k).alias("precision"),
            F.avg(F.col("hits") / F.col("n_rel")).alias("recall"),
            F.count("*").alias("users"),
        )
        .first()
    )
    return {f"precision@{k}": round(row["precision"], 4), f"recall@{k}": round(row["recall"], 4),
            "users_evaluated": row["users"],
            "distinct_books_recommended": recs.select("book_id").distinct().count()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ranks", default=ALS_RANKS)
    parser.add_argument("--regs", default=ALS_REGS)
    parser.add_argument("--max-iter", type=int, default=ALS_MAX_ITER)
    parser.add_argument("--test-ratio", type=float, default=ALS_TEST_RATIO)
    parser.add_argument("--eval-users", type=int, default=ALS_EVAL_USERS,
                        help="users sampled for Precision/Recall")
    parser.add_argument("--candidates", type=int, default=200, help="ALS candidates per user before removing seen books")
    parser.add_argument("--seed", type=int, default=ALS_SEED)
    parser.add_argument("--min-rating", type=int, default=ALS_MIN_RATING,
                        help="minimum rating used as a positive ALS signal (configured via ALS_MIN_RATING)")
    parser.add_argument("--implicit", action="store_true",
                        help="implicit-feedback ALS (ranking-oriented): ratings become confidence weights; "
                             "tuned on validation Precision@10 instead of RMSE")
    parser.add_argument("--alphas", default=ALS_ALPHAS,
                        help="--implicit: confidence scales to try (comma list)")
    args = parser.parse_args()

    spark = get_spark("bookpulse-train")
    spark.sparkContext.setCheckpointDir(str(CHECKPOINT_DIR / "als"))
    started = time.time()

    if not 1 <= args.min_rating <= 5:
        parser.error("--min-rating must be between 1 and 5")
    if not 0 < args.test_ratio < 1:
        parser.error("--test-ratio must be between 0 and 1")
    if args.max_iter < 1 or args.eval_users < 1:
        parser.error("--max-iter and --eval-users must be positive")
    ratings = spark.read.parquet(str(PARQUET_DIR / "ratings"))
    # Split only positive ratings; low ratings remain in `ratings` for seen-book filtering at serve time.
    train, test, n_streamed = split_train_test(ratings, args.test_ratio, args.seed, args.min_rating)
    train, test = train.cache(), test.cache()
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

    # Baselines for RMSE
    global_mean = train.agg(F.avg("rating")).first()[0]
    item_mean = train.groupBy("book_id").agg(F.avg("rating").alias("item_mean"))
    with_mean = test.join(item_mean, "book_id", "left")
    rmse = {
        "baseline_global_mean": evaluator.evaluate(with_mean.withColumn("prediction", F.lit(float(global_mean)))),
        "baseline_item_mean": evaluator.evaluate(
            with_mean.withColumn("prediction", F.coalesce("item_mean", F.lit(float(global_mean))))),
    }

    # ALS is the Estimator; TrainValidationSplit picks rank/regParam on a validation slice of train
    # Unconstrained implicit factors let the fixed-item user fold-in use the exact ALS normal equation.
    als = ALS(userCol="user_id", itemCol="book_id", ratingCol="rating", maxIter=args.max_iter,
              coldStartStrategy="drop", nonnegative=not args.implicit, seed=args.seed)
    ranks = [int(x) for x in args.ranks.split(",")]
    regs = [float(x) for x in args.regs.split(",")]
    fit_started = time.time()
    if args.implicit:
        # Implicit ALS predicts a preference score, not stars, so RMSE does not apply: judge it by ranking.
        # Each combination is scored on a validation slice of train; the test set stays untouched.
        def implicit_als(rank, reg, alpha):
            return als.copy().setImplicitPrefs(True).setRank(rank).setRegParam(reg).setAlpha(alpha)

        combos = [(r, g, a) for r in ranks for g in regs
                  for a in (float(x) for x in args.alphas.split(","))]
        tried = []
        if len(combos) > 1:
            fit_part, val, _ = split_train_test(train, 0.2, args.seed + 1, args.min_rating)
            fit_part, val = fit_part.cache(), val.cache()
            val_users = val.select("user_id").distinct().orderBy(F.rand(args.seed)).limit(args.eval_users).cache()
            val_relevant = val.filter(F.col("rating") >= args.min_rating).join(
                val_users, "user_id").select("user_id", "book_id")
            fit_seen = (ratings.join(test.select("user_id", "book_id"), ["user_id", "book_id"], "left_anti")
                        .join(val.select("user_id", "book_id"), ["user_id", "book_id"], "left_anti")
                        .select("user_id", "book_id"))
            for rank, reg, alpha in combos:
                started_combo = time.time()
                candidate = implicit_als(rank, reg, alpha).fit(fit_part)
                recs = top_k_unseen(explode_recs(candidate.recommendForUserSubset(val_users, args.candidates)),
                                    fit_seen, K)
                m = ranking_metrics(recs, val_relevant, K)
                tried.append({"rank": rank, "regParam": reg, "alpha": alpha,
                              "validation_precision@10": m["precision@10"], "validation_recall@10": m["recall@10"],
                              "seconds": round(time.time() - started_combo, 1)})
                print(tried[-1], flush=True)
            best = max(tried, key=lambda t: t["validation_precision@10"])
        else:
            rank, reg, alpha = combos[0]
            best = {"rank": rank, "regParam": reg, "alpha": alpha}
            tried.append(best)
        model = implicit_als(best["rank"], best["regParam"], best["alpha"]).fit(train)
    else:
        grid = ParamGridBuilder().addGrid(als.rank, ranks).addGrid(als.regParam, regs).build()
        tvs = TrainValidationSplit(estimator=als, estimatorParamMaps=grid, evaluator=evaluator,
                                   trainRatio=0.8, seed=args.seed)
        tvs_model = tvs.fit(train)
        model = tvs_model.bestModel
        tried = [{**{p.name: v for p, v in params.items()}, "validation_rmse": round(m, 4)}
                 for params, m in zip(grid, tvs_model.validationMetrics)]
        best = min(tried, key=lambda t: t["validation_rmse"])
        rmse["als"] = evaluator.evaluate(model.transform(test))
    fit_seconds = time.time() - fit_started
    rmse = {name: round(value, 4) for name, value in rmse.items()}

    # Ranking metrics on a sample of test users
    eval_users = test.select("user_id").distinct().orderBy(F.rand(args.seed)).limit(args.eval_users).cache()
    relevant = test.filter(F.col("rating") >= args.min_rating).join(
        eval_users, "user_id").select("user_id", "book_id")
    seen = ratings.join(test.select("user_id", "book_id"), ["user_id", "book_id"], "left_anti").select(
        "user_id", "book_id")
    als_recs = top_k_unseen(explode_recs(model.recommendForUserSubset(eval_users, args.candidates)), seen, K)
    popular = (train.groupBy("book_id").agg(F.count("*").alias("score"))
               .orderBy(F.desc("score")).limit(args.candidates))
    pop_recs = top_k_unseen(eval_users.crossJoin(popular), seen, K)
    ranking = {"als": ranking_metrics(als_recs, relevant, K),
               "baseline_popular": ranking_metrics(pop_recs, relevant, K)}

    model_name = "als_implicit" if args.implicit else "als"
    model_version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    model_root = MODEL_DIR / model_name
    version_path = model_root / "versions" / model_version
    version_path.mkdir(parents=True, exist_ok=False)
    model.write().save(str(version_path / "spark_model"))
    manifest = {
        "model_version": model_version,
        "model_name": model_name,
        "implicit": args.implicit,
        "implicitPrefs": args.implicit,
        "nonnegative": not args.implicit,
        "min_rating": args.min_rating,
        "rating_threshold": args.min_rating,
        "alpha": best.get("alpha") if args.implicit else None,
        "regParam": best["regParam"],
        "rank": best["rank"],
        "maxIter": args.max_iter,
        "trained_at": datetime.now(timezone.utc).isoformat(),
    }
    if args.implicit:
        item_rows = model.itemFactors.orderBy("id").collect()
        item_ids = np.asarray([row["id"] for row in item_rows], dtype=np.int32)
        item_factors = np.asarray([row["features"] for row in item_rows], dtype=np.float32)
        # Gramian dùng toàn bộ itemFactors cố định, tích lũy chính xác hơn ở float64.
        gram = item_factors.astype(np.float64).T @ item_factors.astype(np.float64)
        np.save(version_path / "item_ids.npy", item_ids)
        np.save(version_path / "item_factors.npy", item_factors)
        np.save(version_path / "YtY.npy", gram)
    (version_path / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    pointer_path = model_root / "current.json"
    temporary_pointer = pointer_path.with_suffix(".json.tmp")
    temporary_pointer.write_text(json.dumps({"path": f"versions/{model_version}"}), encoding="utf-8")
    temporary_pointer.replace(pointer_path)
    save_report("train_als_implicit" if args.implicit else "train_als", {
        "spark_master": os.getenv("SPARK_MASTER", "local[*]"),
        "implicit": args.implicit,
        "min_rating": args.min_rating,
        "model_version": model_version,
        "rows": {"train": train.count(), "test": test.count(), "streamed_in_train": n_streamed},
        "grid": tried,
        "best_params": best,
        "rmse_test": rmse,
        "ranking_test": ranking,
        "fit_seconds": round(fit_seconds, 1),
        "total_seconds": round(time.time() - started, 1),
        "model_path": str(version_path / "spark_model"),
        "item_ids_path": str(version_path / "item_ids.npy") if args.implicit else None,
        "item_factors_path": str(version_path / "item_factors.npy") if args.implicit else None,
        "YtY_path": str(version_path / "YtY.npy") if args.implicit else None,
    })
    spark.stop()


if __name__ == "__main__":
    main()
