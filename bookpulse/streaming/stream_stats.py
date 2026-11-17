"""Streaming job: Kafka rating events -> validate, de-duplicate -> MongoDB.

Per micro-batch it writes the newest rating for each user-book pair, the books users have seen,
coalesced recommendation jobs, and online evaluation counters. rating_stats is derived from the
current user_ratings state, so re-rating a book cannot inflate its count.

Spark + Kafka + checkpoint gives at-least-once delivery, so a batch can be replayed after a restart.
The last applied batch id is stored in MongoDB. Rating state is applied with event-time ordering,
rating_stats is recomputed from that state, and online_eval protects its event counters with a batch id.

A third query archives every valid event to Parquet (data/parquet/stream_events, partitioned by
event_date): stateless, no watermark, no de-duplication, so a rating that arrives late is NOT lost from
the training data. Spark's file sink records committed files in _spark_metadata, so the archive is
exactly-once. batch/clean.py removes duplicates exactly by event_id and merges the events into the
training data, so the next retrain learns from ratings that arrived through the stream.
(Speed layer drops late events to bound its state; batch layer keeps them: Lambda architecture.)

Scale note: process_valid collects each micro-batch to the driver and writes it with one MongoDB
client. That is fine for this project's event rate; --max-offsets bounds how much one batch can pull
from Kafka (backpressure after downtime). At larger scale the write would move to the executors
(foreachPartition, or pre-aggregating per book in Spark before writing).

Observability: the progress of every query (input rows, rows/s, batch duration, state size, rows
dropped by watermark or as duplicates, current watermark) is accumulated per batch and written to
reports/stream_progress_<timestamp>.json every 10 seconds and when the job stops.
"""
import argparse
import json
import logging
import os
import platform
import time
from datetime import datetime, timezone

from pymongo import UpdateOne
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from common.settings import (
    ALS_MIN_RATING, CHECKPOINT_DIR, KAFKA_BOOTSTRAP, KAFKA_LOG_RETENTION_MS, KAFKA_TOPIC,
    REPORT_DIR, STREAM_EVENTS_DIR, STREAM_MAX_OFFSETS, STREAM_PARTITIONS, STREAM_RESTART_SECONDS,
    STREAM_TRIGGER, STREAM_WATERMARK, get_db, get_spark,
)
from common.mongo_indexes import ensure_indexes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bookpulse-stream")

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("book_id", IntegerType()),
    StructField("rating", IntegerType()),
    StructField("event_time", StringType()),
    StructField("ingested_at", StringType()),
    StructField("simulated", BooleanType()),
])
TOP_K = 10


def already_applied(db, key, batch_id):
    """True when this micro-batch was already written (Spark replays the last batch after a crash).

    Only the last batch can be replayed, so batch_id is last_batch or last_batch + 1. A smaller batch_id
    means the checkpoint and MongoDB no longer belong together (checkpoint deleted or moved, MongoDB
    kept): skipping would silently drop every batch up to last_batch, so stop with an error instead.
    The mirror case (MongoDB reset, checkpoint kept: no state but batch_id > 0) stops as well.
    """
    state = db.stream_state.find_one({"_id": key})
    if state is None and batch_id > 0:
        raise RuntimeError(f"{key}: batch {batch_id} but no stream_state in MongoDB: the checkpoint was kept while "
                           "MongoDB was reset; batches 0.." f"{batch_id - 1} would be lost silently. Reset both with "
                           "scripts/reset_streaming.py")
    last = -1 if state is None else state.get("last_batch", -1)
    if batch_id < last:
        raise RuntimeError(f"{key}: batch {batch_id} < last_batch {last} in MongoDB: checkpoint and "
                           "stream_state do not match; reset both with scripts/reset_streaming.sh")
    return last >= batch_id


def mark_applied(db, key, batch_id, **inc):
    update = {"$set": {"last_batch": batch_id, "updated": datetime.now(timezone.utc)}}
    if inc:
        update["$inc"] = inc
    db.stream_state.update_one({"_id": key}, update, upsert=True)


def latest_rating_update(row, ingested_at):
    """Keep the latest event-time value for a (user, book), using ingestion time to break ties."""
    event_time = row["event_time"]
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    incoming_time = {"$literal": event_time}
    stored_time = {"$ifNull": ["$event_time", epoch]}
    incoming_ingest = {"$literal": ingested_at}
    stored_ingest = {"$ifNull": ["$ingested_at", epoch]}
    incoming_id = {"$literal": row["event_id"]}
    stored_id = {"$ifNull": ["$event_id", ""]}
    is_newer = {"$or": [
        {"$gt": [incoming_time, stored_time]},
        {"$and": [{"$eq": [incoming_time, stored_time]},
                  {"$gt": [incoming_ingest, stored_ingest]}]},
        {"$and": [{"$eq": [incoming_time, stored_time]},
                  {"$eq": [incoming_ingest, stored_ingest]},
                  {"$gte": [incoming_id, stored_id]}]},
    ]}
    key = f"{row['user_id']}_{row['book_id']}"
    return UpdateOne({"_id": key}, [{
        "$set": {
            "user_id": {"$literal": row["user_id"]},
            "book_id": {"$literal": row["book_id"]},
            "rating": {"$cond": [is_newer, {"$literal": row["rating"]}, "$rating"]},
            "event_time": {"$cond": [is_newer, incoming_time, "$event_time"]},
            "ingested_at": {"$cond": [is_newer, incoming_ingest, "$ingested_at"]},
            "event_id": {"$cond": [is_newer, incoming_id, "$event_id"]},
        },
    }], upsert=True)


def latest_rows_by_pair(rows):
    """Keep one deterministic, newest event for each user-book pair in a micro-batch."""
    latest = {}
    for row in rows:
        key = (int(row["user_id"]), int(row["book_id"]))
        event_order = (row["event_time"], row["ingested_at"], row["event_id"])
        current = latest.get(key)
        if current is None or event_order > (
                current["event_time"], current["ingested_at"], current["event_id"]):
            latest[key] = row
    return [latest[key] for key in sorted(latest)]


def latest_event_by_user(rows):
    """Return each user's newest event tuple for the coalesced worker job."""
    latest = {}
    for row in rows:
        event = (row["event_time"], row["ingested_at"], row["event_id"])
        user_id = int(row["user_id"])
        if user_id not in latest or event > latest[user_id]:
            latest[user_id] = event
    return latest


def update_online_eval(db, rows, batch_id):
    meta = db.meta.find_one({"_id": "active"})
    if meta is None:
        return
    liked = [r for r in rows if r["rating"] >= ALS_MIN_RATING]
    users = list({r["user_id"] for r in liked})
    top = {d["_id"]: {x["book_id"] for x in d["recs"]}
           for d in db[meta["recs_collection"]].find({"_id": {"$in": users}}, {"recs": {"$slice": TOP_K}})}
    known = [r for r in liked if r["user_id"] in top]
    hits = sum(1 for r in known if r["book_id"] in top[r["user_id"]])
    key = {"_id": f"hit_rate@{TOP_K}"}
    db.online_eval.update_one(key, {"$setOnInsert": {"liked_events": 0, "hits": 0, "cold_start_events": 0,
                                                     "last_batch": -1}}, upsert=True)
    db.online_eval.update_one(
        {**key, "$or": [{"last_batch": {"$lt": batch_id}}, {"last_batch": {"$exists": False}}]},
        {"$inc": {"liked_events": len(known), "hits": hits, "cold_start_events": len(liked) - len(known)},
         "$set": {"model_version": meta["model_version"], "updated": datetime.now(timezone.utc),
                  "last_batch": batch_id}},
    )


def recompute_rating_stats(db, book_ids=None, batch_id=None):
    match = {"book_id": {"$in": sorted(set(book_ids))}} if book_ids is not None else {}
    if book_ids is not None and not match["book_id"]["$in"]:
        return
    if book_ids is None:
        db.rating_stats.update_many({}, {"$set": {"stale": True}})
    pipeline = [
        {"$match": match} if match else {"$match": {"rating": {"$exists": True}}},
        {"$group": {"_id": "$book_id", "rating_sum": {"$sum": "$rating"},
                    "n_ratings": {"$sum": 1}}},
    ]
    now = datetime.now(timezone.utc)
    operations = []
    for row in db.user_ratings.aggregate(pipeline, allowDiskUse=True):
        n_ratings = int(row["n_ratings"])
        values = {
            "rating_sum": int(row["rating_sum"]),
            "n_ratings": n_ratings,
            "avg_rating": float(row["rating_sum"]) / n_ratings,
            "stale": False,
            "updated": now,
        }
        if batch_id is not None:
            values["last_batch"] = batch_id
        operations.append(UpdateOne({"_id": int(row["_id"])}, {"$set": values}, upsert=True))
    if operations:
        db.rating_stats.bulk_write(operations, ordered=False)


def ensure_topic(partitions):
    """Spark stops with UnknownTopicOrPartitionException when the topic does not exist yet (Kafka only
    auto-creates it on the producer's first send), so create it before subscribing."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        if KAFKA_TOPIC not in admin.list_topics():
            admin.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=partitions, replication_factor=1)])
            print(f"created topic '{KAFKA_TOPIC}' with {partitions} partitions")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def apply_valid_rows(db, rows, batch_id):
    """Write one micro-batch to the speed view so that writing the same batch twice changes nothing.

    user_ratings keeps one newest event per user-book pair. rating_stats is recomputed from that
    collection for touched books, so replaying a batch or rating a pair again cannot inflate counts.
    """
    if not rows:
        return
    now = datetime.now(timezone.utc)
    touched_books = sorted({int(row["book_id"]) for row in rows})
    latest_rows = latest_rows_by_pair(rows)
    db.user_seen.bulk_write([latest_rating_update(row, now) for row in latest_rows], ordered=False)
    # The query is the single live-rating path: latest rating per pair, then one coalesced job per user.
    db.user_ratings.bulk_write([latest_rating_update(r, now) for r in latest_rows], ordered=False)
    recompute_rating_stats(db, touched_books, batch_id)
    latest_by_user = latest_event_by_user(latest_rows)
    db.recommendation_jobs.bulk_write([
        UpdateOne(
            {"user_id": user_id, "status": "pending"},
            {"$setOnInsert": {"created_at": now, "retry_count": 0},
             "$set": {"requested_at": now, "updated_at": now,
                      "latest_event_time": event[0], "latest_event_id": event[2]}},
            upsert=True,
        )
        for user_id, event in latest_by_user.items()
    ], ordered=False)
    update_online_eval(db, rows, batch_id)


def process_valid(df, batch_id):
    db = get_db()
    if already_applied(db, "valid_events", batch_id):
        return
    rows = df.select("user_id", "book_id", "rating", "event_time", "ingested_at", "event_id").collect()
    apply_valid_rows(db, rows, batch_id)
    mark_applied(db, "valid_events", batch_id, events=len(rows))


def process_rejected(df, batch_id):
    db = get_db()
    if not already_applied(db, "rejected_events", batch_id):
        mark_applied(db, "rejected_events", batch_id, events=df.count())


def find_lost_offsets(start_offsets, earliest_offsets):
    """Return partition gaps where Kafka retention moved past Spark's next requested offset."""
    lost = {}
    for partition, start in start_offsets.items():
        partition = str(partition)
        earliest = earliest_offsets.get(partition, earliest_offsets.get(int(partition)))
        if earliest is not None and int(start) < int(earliest):
            lost[partition] = {
                "requested_offset": int(start),
                "earliest_available_offset": int(earliest),
                "lost_records": int(earliest) - int(start),
            }
    return lost


def record_offset_gaps(db, progress, earliest_offsets, topic=KAFKA_TOPIC):
    """Persist each observed gap once and emit an explicit error log."""
    sources = progress.get("sources", [])
    all_gaps = {}
    for source in sources:
        start = source.get("startOffset") or {}
        start = start.get(topic, {}) if isinstance(start, dict) else {}
        gaps = find_lost_offsets(start, earliest_offsets)
        for partition, values in gaps.items():
            all_gaps[partition] = values
            incident_id = (
                f"kafka_offset_gap:{progress.get('id', 'unknown')}:"
                f"{progress.get('batchId', 'unknown')}:{partition}"
            )
            db.stream_state.update_one(
                {"_id": incident_id},
                {"$setOnInsert": {
                    "kind": "kafka_offset_gap",
                    "query_id": progress.get("id"),
                    "batch_id": progress.get("batchId"),
                    "topic": topic,
                    "partition": int(partition),
                    **values,
                    "detected_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            )
            logger.error(
                "Kafka offsets lost: topic=%s partition=%s requested=%d earliest=%d lost=%d",
                topic, partition, values["requested_offset"],
                values["earliest_available_offset"], values["lost_records"],
            )
    return all_gaps


def kafka_earliest_offsets(topic):
    """Read Kafka's current beginning offsets for each partition without joining a consumer group."""
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=1000,
    )
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
        beginnings = consumer.beginning_offsets(topic_partitions)
        return {str(partition.partition): offset for partition, offset in beginnings.items()}
    finally:
        consumer.close()


class ProgressLog:
    """Accumulates StreamingQueryProgress per query and batch id, and writes a JSON summary."""

    def __init__(self, path, settings):
        self.path, self.settings, self.batches = path, settings, {}

    def collect(self, queries):
        new_progress = []
        for name, q in queries.items():
            for p in q.recentProgress:  # Spark keeps the last 100 updates; we poll every 10 s
                batches = self.batches.setdefault(name, {})
                if p["batchId"] not in batches:
                    new_progress.append((name, p))
                    batches[p["batchId"]] = p
        return new_progress

    def write(self, queries):
        out = {
            "settings": self.settings,
            "written": datetime.now(timezone.utc).isoformat(),
            "run_metadata": {
                "commit_hash": os.getenv("BOOKPULSE_COMMIT_HASH") or None,
                "working_tree_dirty": os.getenv("BOOKPULSE_WORKTREE_DIRTY"),
                "python_version": platform.python_version(),
                "configuration": self.settings,
            },
            "queries": {},
        }
        for name, q in queries.items():
            progress = [self.batches.get(name, {})[b] for b in sorted(self.batches.get(name, {}))]
            ops = [op for p in progress for op in p.get("stateOperators", [])]
            out["queries"][name] = {
                "query_id": str(q.id),
                "batches": len(progress),
                "input_rows": sum(p["numInputRows"] for p in progress),
                "max_input_rows_per_second": max((p.get("inputRowsPerSecond") or 0 for p in progress), default=0),
                "max_processed_rows_per_second": max((p.get("processedRowsPerSecond") or 0 for p in progress),
                                                     default=0),
                "max_batch_duration_ms": max((p["durationMs"].get("triggerExecution", 0) for p in progress),
                                             default=0),
                "rows_dropped_by_watermark": sum(op.get("numRowsDroppedByWatermark", 0) for op in ops),
                "duplicates_dropped": sum(op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
                                          for op in ops),
                "state_rows_now": sum(op.get("numRowsTotal", 0) for op in (progress[-1].get("stateOperators", [])
                                                                          if progress else [])),
                "watermark_now": progress[-1].get("eventTime", {}).get("watermark") if progress else None,
                "last_progress": progress[-1] if progress else None,
            }
        self.path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watermark", default=STREAM_WATERMARK, help="how late an event may arrive")
    parser.add_argument("--trigger", default=STREAM_TRIGGER)
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--run-seconds", type=int, default=0, help="0 = run until Ctrl+C")
    parser.add_argument("--partitions", type=int, default=STREAM_PARTITIONS,
                        help="partitions if the topic has to be created")
    parser.add_argument("--max-offsets", type=int, default=STREAM_MAX_OFFSETS,
                        help="maxOffsetsPerTrigger: most Kafka records one micro-batch reads (backpressure)")
    args = parser.parse_args()
    if args.run_seconds < 0 or args.max_offsets < 1 or args.partitions < 1:
        parser.error("--run-seconds cannot be negative; --max-offsets and --partitions must be positive")

    ensure_topic(args.partitions)
    db = get_db()
    ensure_indexes(db)
    recompute_rating_stats(db)
    spark = get_spark("bookpulse-stream", kafka=True)
    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
        .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingested_at", F.current_timestamp())
    )
    is_valid = F.coalesce(
        F.col("event_id").isNotNull() & F.col("user_id").isNotNull() & F.col("book_id").isNotNull()
        & F.col("event_time").isNotNull() & F.col("rating").between(1, 5),
        F.lit(False),
    )
    valid = (events.filter(is_valid)
             .withWatermark("event_time", args.watermark)
             .dropDuplicatesWithinWatermark(["event_id"]))
    rejected = events.filter(~is_valid)
    valid_events = events.filter(is_valid).withColumn("event_date", F.to_date("event_time"))

    def start_queries():
        return {
            "valid": valid.writeStream.queryName("valid").foreachBatch(process_valid)
            .option("checkpointLocation", str(CHECKPOINT_DIR / "stream_valid"))
            .trigger(processingTime=args.trigger).start(),
            "rejected": rejected.writeStream.queryName("rejected").foreachBatch(process_rejected)
            .option("checkpointLocation", str(CHECKPOINT_DIR / "stream_rejected"))
            .trigger(processingTime=args.trigger).start(),
            # Archive every valid event, including late events; batch clean deduplicates by event_id.
            "archive": valid_events.writeStream.queryName("archive").format("parquet").outputMode("append")
            .partitionBy("event_date")
            .option("path", str(STREAM_EVENTS_DIR))
            .option("checkpointLocation", str(CHECKPOINT_DIR / "stream_events_archive"))
            .trigger(processingTime=args.trigger).start(),
        }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    log = ProgressLog(REPORT_DIR / f"stream_progress_{time.strftime('%Y%m%d_%H%M%S')}.json",
                      {"watermark": args.watermark, "trigger": args.trigger,
                       "max_offsets": args.max_offsets, "topic": KAFKA_TOPIC,
                       "kafka_log_retention_ms": KAFKA_LOG_RETENTION_MS})
    print(f"streaming from '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP}; Ctrl+C to stop; progress -> {log.path.name}")
    started = time.time()
    retry = 0
    try:
        while True:
            queries = start_queries()
            try:
                while True:
                    failed = {name: query.exception() for name, query in queries.items()
                              if not query.isActive}
                    if failed:
                        reason = next((error for error in failed.values() if error), None)
                        if reason:
                            raise reason
                        raise RuntimeError(f"Streaming queries stopped unexpectedly: {', '.join(failed)}")
                    time.sleep(10)
                    new_progress = log.collect(queries)
                    valid_progress = [progress for name, progress in new_progress if name == "valid"]
                    if valid_progress:
                        earliest = kafka_earliest_offsets(KAFKA_TOPIC)
                        for progress in valid_progress:
                            record_offset_gaps(db, progress, earliest)
                    log.write(queries)
                    for name, query in queries.items():
                        progress = query.lastProgress
                        if progress and progress["numInputRows"]:
                            dropped = sum(op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
                                          for op in progress.get("stateOperators", []))
                            print(f"[{name}] batch {progress['batchId']}: {progress['numInputRows']} rows, "
                                  f"{progress.get('processedRowsPerSecond', 0):.1f} rows/s, "
                                  f"duplicates dropped {dropped}")
                    if args.run_seconds and time.time() - started > args.run_seconds:
                        return
            except KeyboardInterrupt:
                break
            except Exception:
                retry += 1
                logger.exception("Streaming query failed; restarting from its checkpoint (attempt %d)", retry)
                log.collect(queries)
                log.write(queries)
                time.sleep(STREAM_RESTART_SECONDS)
            finally:
                for query in queries.values():
                    if query.isActive:
                        query.stop()
    except KeyboardInterrupt:
        pass
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
