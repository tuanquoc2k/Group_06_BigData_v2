"""Trace one in-top-10 and one outside-top-10 positive rating through Kafka to the app view."""
import argparse
import json
import time
from datetime import datetime, timezone
from urllib.request import urlopen

import numpy as np
from pymongo import MongoClient

from common.events import make_producer, rating_event
from common.foldin import load_model
from common.recs import visible_top10
from common.settings import (
    ALS_MODEL_NAME, KAFKA_TOPIC, MODEL_DIR, MONGO_DB, MONGO_URI, save_report,
)


def top10_change(before, after):
    before_ids = [int(item["book_id"]) for item in before]
    after_ids = [int(item["book_id"]) for item in after]
    union = set(before_ids) | set(after_ids)
    common = set(before_ids) & set(after_ids)
    return {
        "before": before_ids,
        "after": after_ids,
        "jaccard": len(common) / len(union) if union else 1.0,
        "mean_absolute_rank_shift": (
            float(np.mean([abs(before_ids.index(book) - after_ids.index(book))
                           for book in common])) if common else None
        ),
    }


def wait_for_snapshot(db, user_id, event_id, deadline):
    while time.monotonic() < deadline:
        live = db.recs_live.find_one(
            {"_id": user_id, "trigger_event_id": event_id},
        )
        if live:
            job = db.recommendation_jobs.find_one(
                {"user_id": user_id, "latest_event_id": event_id, "status": "done"},
                {"status": 1, "latest_event_id": 1},
            )
            ratings = db.user_ratings.count_documents({"user_id": user_id})
            if job and ratings:
                return live, job, ratings
        time.sleep(0.1)
    raise TimeoutError(f"Timed out waiting for fold-in snapshot for user {user_id}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--seed-ratings", type=int, default=5)
    parser.add_argument("--user-id-start", type=int, default=-1_100_000_000)
    args = parser.parse_args()
    if args.timeout_seconds < 1 or args.seed_ratings < 1:
        parser.error("timeout and seed rating count must be positive")
    try:
        with urlopen("http://127.0.0.1:8600/status", timeout=3) as response:
            stream_running = json.load(response).get("stream_running", False)
    except (OSError, ValueError) as exc:
        raise RuntimeError("Cannot verify the stream job through the Spark control server") from exc
    if not stream_running:
        raise RuntimeError("Start streaming/stream_stats.py before running the fold-in diagnosis")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]
    active = db.meta.find_one({"_id": "active"}) or {}
    version = active.get("model_version")
    if not version or not active.get("foldin_available"):
        raise RuntimeError("The active implicit model is not available for fold-in")
    model = load_model(
        MODEL_DIR / ALS_MODEL_NAME / "versions" / version,
        active_model_version=version,
    )
    if args.seed_ratings < model.min_ratings:
        parser.error(f"--seed-ratings must be at least model.min_ratings ({model.min_ratings})")

    test_users = [args.user_id_start, args.user_id_start - 1]
    for user_id in test_users:
        occupied = (
            db.user_ratings.find_one({"user_id": user_id})
            or db.user_seen.find_one({"user_id": user_id})
            or db.recommendation_jobs.find_one({"user_id": user_id})
            or db.recs_live.find_one({"_id": user_id})
        )
        if occupied:
            raise RuntimeError(f"Refusing to reuse existing test user ID {user_id}")

    # Pick varied seed books from the fixed item factors, not from titles or generated labels.
    norms = np.linalg.norm(model.factors, axis=1)
    chosen = [int(np.argmax(norms))]
    while len(chosen) < min(args.seed_ratings, len(model.item_ids)):
        selected = model.factors[chosen]
        normalized = selected / np.maximum(np.linalg.norm(selected, axis=1, keepdims=True), 1e-12)
        candidate_similarity = model.factors @ normalized.T
        similarity = np.max(candidate_similarity, axis=1)
        similarity[chosen] = np.inf
        chosen.append(int(np.argmin(similarity)))
    if args.seed_ratings > len(chosen):
        raise ValueError(f"Model has only {len(model.item_ids)} item factors")
    seed_book_ids = [int(model.item_ids[index]) for index in chosen[:args.seed_ratings]]

    producer = make_producer()
    report_cases = []
    completed_users = set()
    try:
        for case_name, user_id in zip(("in_top10", "outside_top10"), test_users):
            seed_events = []
            for book_id in seed_book_ids:
                event = rating_event(user_id, book_id, 5)
                producer.send(KAFKA_TOPIC, key=user_id, value=event).get(timeout=args.timeout_seconds)
                seed_events.append(event)
                time.sleep(0.01)
            seed_live, seed_job, seeded_count = wait_for_snapshot(
                db, user_id, seed_events[-1]["event_id"],
                time.monotonic() + args.timeout_seconds,
            )
            if seeded_count < args.seed_ratings:
                raise RuntimeError(f"Seed profile incomplete for test user {user_id}: {seeded_count}")

            before_app = visible_top10(db, user_id, k=10)["top10"]
            before_vector = np.asarray(seed_live.get("user_vector"), dtype=np.float64)
            seen = set(seed_book_ids)
            if case_name == "in_top10":
                if not before_app:
                    raise RuntimeError("No current top-10 item available for in-top-10 test")
                target_book = int(before_app[0]["book_id"])
            else:
                available = [
                    index for index, book_id in enumerate(model.item_ids)
                    if int(book_id) not in seen and int(book_id) not in
                    {int(item["book_id"]) for item in before_app}
                ]
                if not available:
                    raise RuntimeError("No outside-top-10 item available for test")
                scores = model.factors[available] @ before_vector
                target_book = int(model.item_ids[available[int(np.argmin(scores))]])

            rating_event_data = rating_event(user_id, target_book, 5)
            sent_at = time.perf_counter()
            producer.send(KAFKA_TOPIC, key=user_id, value=rating_event_data).get(
                timeout=args.timeout_seconds
            )
            live, job, rating_count = wait_for_snapshot(
                db, user_id, rating_event_data["event_id"],
                time.monotonic() + args.timeout_seconds,
            )
            after_app = visible_top10(db, user_id, k=10)["top10"]
            if target_book in [int(item["book_id"]) for item in after_app]:
                raise RuntimeError(f"Rated book {target_book} is still visible in the app top-10")
            if not job or job.get("latest_event_id") != rating_event_data["event_id"]:
                raise RuntimeError(f"Recommendation job did not finish for event {rating_event_data['event_id']}")

            report_cases.append({
                "case": case_name,
                "user_id": user_id,
                "seed_event_ids": [event["event_id"] for event in seed_events],
                "trigger_event_id": rating_event_data["event_id"],
                "target_book_id": target_book,
                "was_in_pre_rating_top10": target_book in
                    {int(item["book_id"]) for item in before_app},
                "rating_rows_after_trigger": rating_count,
                "job_status": job["status"],
                "live_source": live.get("source"),
                "model_version": live.get("model_version"),
                "positive_ratings": live.get("positive_ratings"),
                "unknown_positive_ratings": live.get("unknown_positive_ratings"),
                "vector_delta_norm": live.get("vector_delta_norm"),
                "vector_cosine_previous": live.get("vector_cosine_previous"),
                "worker_seconds": {
                    "fold_in": live.get("fold_in_seconds"),
                    "ranking": live.get("ranking_seconds"),
                    "mongo_snapshot_write": live.get("mongo_snapshot_write_seconds"),
                },
                "send_to_live_seconds": time.perf_counter() - sent_at,
                "top10_change": top10_change(before_app, after_app),
                "app_top10_excludes_rated_book": True,
            })
            completed_users.add(user_id)

        report = {
            "model_version": version,
            "simulated_events": True,
            "seed_book_ids": seed_book_ids,
            "cases": report_cases,
            "cleanup": "test user ratings/jobs/seen/live rows removed; Kafka and archive events retained",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        save_report("foldin_diagnostic", report)
    finally:
        producer.close()
        if completed_users:
            completed_ids = sorted(completed_users)
            user_filter = {"user_id": {"$in": completed_ids}}
            for collection in (db.user_ratings, db.user_seen, db.recommendation_jobs):
                collection.delete_many(user_filter)
            db.recs_live.delete_many({"_id": {"$in": completed_ids}})
        client.close()


if __name__ == "__main__":
    main()
