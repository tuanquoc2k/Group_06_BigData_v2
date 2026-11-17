"""Idempotent MongoDB indexes used by the streaming and serving paths."""
from pymongo import ASCENDING


def ensure_indexes(db):
    db.user_seen.create_index("user_id")
    db.user_ratings.create_index(
        [("user_id", ASCENDING), ("book_id", ASCENDING)],
        unique=True,
    )
    db.recommendation_jobs.create_index(
        [("status", ASCENDING), ("created_at", ASCENDING)],
    )
    db.recommendation_jobs.create_index(
        [("user_id", ASCENDING)],
        unique=True,
        partialFilterExpression={"status": "pending"},
    )
    db.recommendation_jobs.create_index(
        [("status", ASCENDING), ("locked_at", ASCENDING)],
    )
