"""Poll MongoDB recommendation jobs and compute implicit-ALS user fold-in recommendations."""
import logging
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone

from pymongo import ASCENDING, ReturnDocument

from common.foldin import fold_in, load_model, recommend_from_vector
from common.mongo_indexes import ensure_indexes
from common.settings import (
    ALS_MODEL_NAME, FOLD_IN_DEBOUNCE_SECONDS, FOLD_IN_JOB_TIMEOUT_SECONDS, FOLD_IN_MAX_RETRIES,
    FOLD_IN_POLL_SECONDS, FOLD_IN_TOP_K, MODEL_DIR, MONGO_URI, MONGO_DB,
)

POLL_SECONDS = FOLD_IN_POLL_SECONDS
DEBOUNCE_SECONDS = FOLD_IN_DEBOUNCE_SECONDS
JOB_TIMEOUT_SECONDS = FOLD_IN_JOB_TIMEOUT_SECONDS
MAX_RETRIES = FOLD_IN_MAX_RETRIES
TOP_N = FOLD_IN_TOP_K

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bookpulse-foldin")


class FactorStore:
    def __init__(self):
        self.model = None
        self.version = None

    def load(self, version):
        path = MODEL_DIR / ALS_MODEL_NAME / "versions" / version
        self.model = load_model(path, active_model_version=version)
        if self.model.version != version:
            raise ValueError(f"Fold-in artifact is not compatible with active implicit model {version}")
        self.version = self.model.version
        log.info("Loaded model %s (%d item factors, rank %d)", version,
                 len(self.model.item_ids), self.model.rank)


def claim_job(db, now):
    stale_before = now - timedelta(seconds=JOB_TIMEOUT_SECONDS)
    claim_token = uuid.uuid4().hex
    db.recommendation_jobs.update_many(
        {"status": "processing", "locked_at": {"$lte": stale_before}, "retry_count": {"$gte": MAX_RETRIES}},
        {"$set": {"status": "failed", "updated_at": now, "last_error": "processing timeout; retries exhausted"},
         "$unset": {"claim_token": "", "locked_at": ""}})
    return db.recommendation_jobs.find_one_and_update(
        {"retry_count": {"$lt": MAX_RETRIES},
         "$or": [
             {"status": "pending", "requested_at": {"$lte": now - timedelta(seconds=DEBOUNCE_SECONDS)}},
             {"status": "processing", "locked_at": {"$lte": stale_before}},
         ]},
        {"$set": {"status": "processing", "locked_at": now, "processing_started_at": now,
                  "updated_at": now, "claim_token": claim_token},
         "$inc": {"retry_count": 1}},
        sort=[("created_at", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )


def utc_datetime(value):
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def build_recommendations(db, factors, user_id, model_version, profile_collection,
                          trigger_event_time=None, trigger_event_id=None,
                          job_id=None, claim_token=None):
    model = factors.model if hasattr(factors, "model") else factors
    active = db.meta.find_one({"_id": "active"})
    if not active or active.get("model_version") != model_version:
        log.warning("Skip fold-in for user %s because model %s is no longer active",
                    user_id, model_version)
        return 0

    # A worker whose lease expired must not publish after another worker reclaimed
    # the job. The token is unique for each atomic claim attempt.
    if job_id is not None and claim_token is not None:
        owner = db.recommendation_jobs.find_one(
            {"_id": job_id, "status": "processing", "claim_token": claim_token},
            {"_id": 1},
        )
        if owner is None:
            log.warning("Skip fold-in for user %s: job %s lease is no longer owned", user_id, job_id)
            return 0

    rows = list(db.user_ratings.find(
        {"user_id": user_id},
        {"book_id": 1, "rating": 1, "event_time": 1, "ingested_at": 1, "event_id": 1, "_id": 0}))
    previous_live = db.recs_live.find_one(
        {"_id": user_id, "model_version": model_version},
        {"user_vector": 1, "updated_at": 1, "source": 1, "_id": 0},
    ) or {}
    previous_snapshot_at = utc_datetime(previous_live.get("updated_at"))
    profile = db[profile_collection].find_one(
        {"_id": user_id, "model_version": model_version},
        {"positive_ratings": 1, "seen_book_ids": 1, "_id": 0}) or {}
    seen = {int(book_id) for book_id in profile.get("seen_book_ids", [])}
    seen.update(int(row["book_id"]) for row in rows)
    positive_by_book = {
        int(row["book_id"]): int(row["rating"])
        for row in profile.get("positive_ratings", [])
        if int(row["rating"]) >= model.min_rating
    }
    for row in rows:
        book_id, rating = int(row["book_id"]), int(row["rating"])
        if rating >= model.min_rating:
            positive_by_book[book_id] = rating
        else:
            positive_by_book.pop(book_id, None)
    positive = list(positive_by_book.items())
    stream_positive_count = sum(
        int(row["rating"]) >= model.min_rating
        and (previous_snapshot_at is None or
             (utc_datetime(row.get("ingested_at") or row.get("event_time"))
              and utc_datetime(row.get("ingested_at") or row.get("event_time")) > previous_snapshot_at))
        for row in rows
    )
    if stream_positive_count < model.min_ratings:
        log.info(
            "Waiting for new streamed positive ratings user=%s count=%d required=%d; keeping prior snapshot",
            user_id, stream_positive_count, model.min_ratings,
        )
        return 0
    foldin_started = time.perf_counter()
    vector = fold_in(positive, model)
    foldin_seconds = time.perf_counter() - foldin_started
    ranking_started = time.perf_counter()
    ranked = recommend_from_vector(vector, seen, model, TOP_N) if vector is not None else []
    previous_vector = previous_live.get("user_vector")
    vector_delta_norm = None
    vector_cosine_previous = None
    if vector is not None and previous_vector is not None:
        import numpy as np

        previous_vector = np.asarray(previous_vector, dtype=np.float64)
        vector_delta_norm = float(np.linalg.norm(vector - previous_vector))
        denominator = float(np.linalg.norm(vector) * np.linalg.norm(previous_vector))
        vector_cosine_previous = (
            float(np.dot(vector, previous_vector) / denominator) if denominator else None
        )

    source = "als-foldin" if vector is not None else "batch-fallback"
    if not ranked:
        log.info("No personalized ranked candidates for user %s; using filtered batch/popular fallback",
                 user_id)
        batch_doc = db[active["recs_collection"]].find_one(
            {"_id": user_id}, {"recs": 1, "fallback": 1, "_id": 0}
        ) if active.get("recs_collection") else None
        fallback_ids = []
        seen_candidates = set(seen)
        for item in (batch_doc or {}).get("recs", []):
            book_id = int(item["book_id"])
            if book_id not in seen_candidates and book_id in model.item_index:
                fallback_ids.append((book_id, item.get("score")))
                seen_candidates.add(book_id)
        for book_id in (batch_doc or {}).get("fallback", []):
            book_id = int(book_id)
            if book_id not in seen_candidates and book_id in model.item_index:
                fallback_ids.append((book_id, None))
                seen_candidates.add(book_id)
        for item in active.get("popular", []):
            book_id = int(item["book_id"])
            if book_id not in seen_candidates and book_id in model.item_index:
                fallback_ids.append((book_id, None))
                seen_candidates.add(book_id)
        ranked = fallback_ids[:TOP_N]
    ranking_seconds = time.perf_counter() - ranking_started

    recs = [{"rank": rank, "book_id": book_id, "score": score}
            for rank, (book_id, score) in enumerate(ranked, 1)]
    recommended_at = datetime.now(timezone.utc)
    latest_event_time = trigger_event_time or max(
        (row["event_time"] for row in rows), default=recommended_at
    )
    trigger_row = next((row for row in rows if row.get("event_id") == trigger_event_id), None)
    latest_ingested_at = (
        trigger_row["ingested_at"] if trigger_row else max(
            (row["ingested_at"] for row in rows), default=recommended_at
        )
    )
    event_time_utc = latest_event_time.replace(tzinfo=timezone.utc) if latest_event_time.tzinfo is None \
        else latest_event_time.astimezone(timezone.utc)
    ingested_at_utc = latest_ingested_at.replace(tzinfo=timezone.utc) if latest_ingested_at.tzinfo is None \
        else latest_ingested_at.astimezone(timezone.utc)
    mongo_write_started = time.perf_counter()
    if job_id is not None and claim_token is not None:
        owner = db.recommendation_jobs.find_one(
            {"_id": job_id, "status": "processing", "claim_token": claim_token},
            {"_id": 1},
        )
        if owner is None:
            log.warning("Skip publishing recommendations for user %s: job %s lease expired during fold-in",
                        user_id, job_id)
            return 0
    db.recs_live.replace_one(
        {"_id": user_id},
        {"_id": user_id, "recs": recs, "source": source,
         "recommendation_source": source, "model_version": model_version, "stale": False,
         "trigger_event_id": trigger_event_id,
         "event_time": latest_event_time, "ingested_at": latest_ingested_at,
         "recommended_at": recommended_at,
         "event_to_recommend_seconds": round((recommended_at - event_time_utc).total_seconds(), 3),
         "ingestion_to_recommend_seconds": round((recommended_at - ingested_at_utc).total_seconds(), 3),
         "positive_ratings": len(positive),
         "stream_positive_ratings": stream_positive_count,
         "fold_in_min_ratings": model.min_ratings,
         "unknown_positive_ratings": sum(
             1 for book_id, _ in positive if int(book_id) not in model.item_index
         ),
         "user_vector": vector.tolist() if vector is not None else None,
         "vector_delta_norm": vector_delta_norm,
         "vector_cosine_previous": vector_cosine_previous,
         "fold_in_seconds": foldin_seconds,
         "ranking_seconds": ranking_seconds,
         "updated_at": recommended_at},
        upsert=True,
    )
    mongo_snapshot_write_seconds = time.perf_counter() - mongo_write_started
    db.recs_live.update_one(
        {"_id": user_id, "model_version": model_version,
         "trigger_event_id": trigger_event_id},
        {"$set": {"mongo_snapshot_write_seconds": mongo_snapshot_write_seconds}},
    )
    active = db.meta.find_one({"_id": "active"}, {"model_version": 1})
    if not active or active.get("model_version") != model_version:
        db.recs_live.update_one(
            {"_id": user_id, "model_version": model_version},
            {"$set": {"stale": True, "stale_at": datetime.now(timezone.utc)}},
        )
    log.info(
        "Fold-in user=%s model=%s source=%s n_positive=%d unknown_positive=%d "
        "vector_delta_norm=%s vector_cosine_previous=%s fold_in_seconds=%.6f "
        "ranking_seconds=%.6f mongo_snapshot_write_seconds=%.6f recs=%d trigger_event_id=%s",
        user_id, model_version, source, len(positive),
        sum(1 for book_id, _ in positive if int(book_id) not in model.item_index),
        vector_delta_norm, vector_cosine_previous, foldin_seconds, ranking_seconds,
        mongo_snapshot_write_seconds, len(recs), trigger_event_id,
    )
    return len(recs)


def run_once(db, factors):
    now = datetime.now(timezone.utc)
    meta = db.meta.find_one({"_id": "active"})
    if (not meta or not meta.get("foldin_available") or meta.get("model_type") != "implicit"
            or not meta.get("user_profiles_collection")):
        return False
    if factors.version != meta["model_version"]:
        factors.load(meta["model_version"])

    job = claim_job(db, now)
    if not job:
        return False
    try:
        current = db.meta.find_one({"_id": "active"})
        if not current or current.get("model_version") != factors.version:
            db.recommendation_jobs.update_one(
                {"_id": job["_id"], "status": "processing", "claim_token": job["claim_token"]},
                {"$set": {"status": "pending", "requested_at": datetime.now(timezone.utc),
                          "updated_at": datetime.now(timezone.utc),
                          "last_error": "active model changed before processing"},
                 "$unset": {"claim_token": "", "locked_at": ""}})
            return True
        written = build_recommendations(db, factors, int(job["user_id"]), factors.version,
                                       meta["user_profiles_collection"],
                                        job.get("latest_event_time"), job.get("latest_event_id"),
                                        job_id=job["_id"], claim_token=job["claim_token"])
        latest_meta = db.meta.find_one({"_id": "active"}, {"model_version": 1})
        if not latest_meta or latest_meta.get("model_version") != factors.version:
            # A model switch raced with this job. Leave it pending so the next
            # iteration loads the new active factors and recomputes the snapshot.
            db.recommendation_jobs.update_one(
                {"_id": job["_id"], "status": "processing", "claim_token": job["claim_token"]},
                {"$set": {"status": "pending", "requested_at": datetime.now(timezone.utc),
                          "updated_at": datetime.now(timezone.utc),
                          "last_error": "active model changed while processing"},
                 "$unset": {"claim_token": "", "locked_at": ""}},
            )
            return True
        completed_at = datetime.now(timezone.utc)
        snapshot = db.recs_live.find_one(
            {"_id": int(job["user_id"]), "model_version": factors.version},
            {"source": 1, "updated_at": 1, "trigger_event_id": 1, "_id": 0},
        ) or {}
        snapshot_updated_for_job = snapshot.get("trigger_event_id") == job.get("latest_event_id")
        if snapshot_updated_for_job and snapshot.get("source") == "als-foldin":
            result = "fold-in-written" if written else "filtered-batch-fallback"
        elif snapshot_updated_for_job:
            result = "batch-fallback"
        else:
            baseline = utc_datetime(snapshot.get("updated_at"))
            rows = list(db.user_ratings.find(
                {"user_id": int(job["user_id"])},
                {"rating": 1, "event_time": 1, "ingested_at": 1, "_id": 0},
            ))
            pending_positive_count = sum(
                int(row["rating"]) >= factors.model.min_rating
                and (baseline is None or
                     (utc_datetime(row.get("ingested_at") or row.get("event_time"))
                      and utc_datetime(row.get("ingested_at") or row.get("event_time")) > baseline))
                for row in rows
            )
            result = (
                "waiting-for-min-ratings"
                if pending_positive_count < factors.model.min_ratings
                else "batch-fallback"
            )
        db.recommendation_jobs.update_one(
            {"_id": job["_id"], "status": "processing", "claim_token": job["claim_token"]},
            {"$set": {"status": "done", "updated_at": completed_at,
                      "processing_completed_at": completed_at,
                      "recommendations_written": written,
                      "result": result},
             "$unset": {"last_error": "", "claim_token": "", "locked_at": ""}})
        log.info("Completed job %s for user %s: %d recommendations", job["_id"], job["user_id"], written)
    except Exception as exc:
        retry_count = int(job.get("retry_count", 1))
        status = "failed" if retry_count >= MAX_RETRIES else "pending"
        db.recommendation_jobs.update_one(
            {"_id": job["_id"], "status": "processing", "claim_token": job.get("claim_token")},
            {"$set": {"status": status, "requested_at": datetime.now(timezone.utc),
                      "updated_at": datetime.now(timezone.utc), "last_error": str(exc)},
             "$unset": {"claim_token": "", "locked_at": ""}})
        traceback.print_exc()
        log.error("Job %s for user %s failed (%s, retry %d/%d)",
                  job["_id"], job["user_id"], exc, retry_count, MAX_RETRIES)
    return True


def main():
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    ensure_indexes(db)
    factors = FactorStore()
    log.info("Recommendation worker started")
    while True:
        try:
            if not run_once(db, factors):
                time.sleep(POLL_SECONDS)
        except Exception:
            log.exception("Recommendation worker iteration failed")
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
