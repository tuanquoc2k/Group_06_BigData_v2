"""Đo độ trễ Kafka → MongoDB recs_live bằng user test mới, rồi dọn đúng dữ liệu test."""
import argparse
import json
import time
from datetime import datetime, timezone
from urllib.request import urlopen

import numpy as np
from pymongo import MongoClient

from common.cleanup import remove_test_users
from common.events import make_producer, rating_event
from common.foldin import load_model
from common.settings import (
    ALS_MODEL_NAME, KAFKA_TOPIC, MODEL_DIR, MONGO_DB, MONGO_URI, save_report,
)


def describe(values):
    if not values:
        return {"count": 0, "mean_seconds": None, "p50_seconds": None,
                "p95_seconds": None, "max_seconds": None}
    return {
        "count": len(values),
        "mean_seconds": float(np.mean(values)),
        "p50_seconds": float(np.percentile(values, 50)),
        "p95_seconds": float(np.percentile(values, 95)),
        "max_seconds": float(max(values)),
    }


def as_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=100,
                        help="distinct controlled users (minimum 100)")
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--poll-seconds", type=float, default=0.1)
    args = parser.parse_args()
    if args.users < 100:
        parser.error("--users must be at least 100")
    if args.timeout_seconds < 1 or args.poll_seconds <= 0:
        parser.error("timeout and poll interval must be positive")
    try:
        with urlopen("http://127.0.0.1:8600/status", timeout=3) as response:
            stream_running = json.load(response).get("stream_running", False)
    except (OSError, ValueError) as exc:
        raise RuntimeError("Cannot verify the stream job through the Spark control server") from exc
    if not stream_running:
        raise RuntimeError("Start streaming/stream_stats.py before measuring live recommendation latency")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]
    active = db.meta.find_one({"_id": "active"}) or {}
    version = active.get("model_version")
    if not active.get("foldin_available") or not version:
        raise RuntimeError("The active model is not available for fold-in")
    model_path = MODEL_DIR / ALS_MODEL_NAME / "versions" / version
    model = load_model(model_path, active_model_version=version)
    book_ids = list(model.item_ids[:args.users])
    if len(book_ids) < args.users:
        raise RuntimeError(f"Need {args.users} model books, found {len(book_ids)}")

    occupied = {
        int(row["user_id"]) for collection in
        (db.user_ratings, db.user_seen, db.recommendation_jobs)
        for row in collection.find({}, {"user_id": 1})
        if row.get("user_id") is not None
    }
    occupied.update(int(row["_id"]) for row in db.recs_live.find({}, {"_id": 1}))
    test_users = []
    candidate = -1_000_000_000
    while len(test_users) < args.users:
        if candidate not in occupied:
            test_users.append(candidate)
        candidate -= 1
    event_rows = []
    send_started = {}
    completed_users = set()
    producer = make_producer()
    try:
        for user_id, book_id in zip(test_users, book_ids):
            event = rating_event(user_id, int(book_id), 5)
            send_started[event["event_id"]] = (time.perf_counter(), datetime.now(timezone.utc))
            producer.send(KAFKA_TOPIC, key=user_id, value=event)
            event_rows.append((user_id, int(book_id), event["event_id"]))
        producer.flush(timeout=args.timeout_seconds)

        pending = {event_id: (user_id, book_id) for user_id, book_id, event_id in event_rows}
        measurements = []
        deadline = time.monotonic() + args.timeout_seconds
        while pending and time.monotonic() < deadline:
            for event_id, (user_id, book_id) in list(pending.items()):
                live = db.recs_live.find_one(
                    {"_id": user_id, "model_version": version, "trigger_event_id": event_id}
                )
                if not live:
                    continue
                observed_at = time.perf_counter()
                sent_at, sent_wall = send_started[event_id]
                user_rating = db.user_ratings.find_one(
                    {"user_id": user_id, "book_id": book_id}, {"ingested_at": 1}
                ) or {}
                ingested_at = user_rating.get("ingested_at") or live.get("ingested_at")
                if ingested_at and ingested_at.tzinfo is None:
                    ingested_at = as_utc(ingested_at)
                sent_wall = as_utc(sent_wall)
                ingestion_seconds = (
                    (ingested_at - sent_wall).total_seconds()
                    if ingested_at and sent_wall else None
                )
                measurements.append({
                    "event_id": event_id,
                    "send_to_live_seconds": observed_at - sent_at,
                    "send_to_ingestion_seconds": ingestion_seconds,
                    "event_to_recommend_seconds": live.get("event_to_recommend_seconds"),
                    "ingestion_to_recommend_seconds": live.get("ingestion_to_recommend_seconds"),
                    "fold_in_seconds": live.get("fold_in_seconds"),
                    "ranking_seconds": live.get("ranking_seconds"),
                    "mongo_snapshot_write_seconds": live.get("mongo_snapshot_write_seconds"),
                    "worker_event_to_live_seconds": live.get("event_to_recommend_seconds"),
                })
                completed_users.add(user_id)
                del pending[event_id]
            if pending:
                time.sleep(args.poll_seconds)

        metrics = {}
        for name in (
            "send_to_live_seconds", "send_to_ingestion_seconds",
            "event_to_recommend_seconds", "ingestion_to_recommend_seconds",
            "fold_in_seconds", "ranking_seconds", "mongo_snapshot_write_seconds",
        ):
            metrics[name] = describe([
                row[name] for row in measurements if row.get(name) is not None
            ])
        report = {
            "model_version": version,
            "topic": KAFKA_TOPIC,
            "simulated_events": True,
            "users_requested": args.users,
            "events_completed": len(measurements),
            "events_timed_out": len(pending),
            "poll_interval_seconds": args.poll_seconds,
            "metrics": metrics,
            "event_measurements": measurements,
            "timed_out_event_ids": list(pending),
            "test_user_ids": test_users,
            "cleanup": "isolated test-user rows are removed; Kafka/archive evidence is retained",
        }
        save_report("stream_latency", report)
        if pending:
            raise TimeoutError(
                f"Only {len(measurements)}/{args.users} test events reached the active live snapshot"
            )
    finally:
        producer.close()
        user_filter = {"user_id": {"$in": test_users}}
        remove_test_users(db, completed_users)
        client.close()


if __name__ == "__main__":
    main()
