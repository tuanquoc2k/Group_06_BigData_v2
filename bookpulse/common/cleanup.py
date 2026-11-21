"""Remove isolated test users written by the measurement scripts and keep the speed view consistent."""
from pymongo import DeleteOne, UpdateOne


def remove_test_users(db, user_ids):
    """Delete the users' stream rows, then recompute rating_stats for the books they had rated.

    rating_stats is derived from user_ratings, so deleting rows without recomputing would leave counts
    that kafka_audit.py reports as a mismatch."""
    user_ids = sorted(user_ids)
    if not user_ids:
        return
    user_filter = {"user_id": {"$in": user_ids}}
    books = sorted({int(row["book_id"]) for row in db.user_ratings.find(user_filter, {"book_id": 1})})
    for collection in (db.user_ratings, db.user_seen, db.recommendation_jobs):
        collection.delete_many(user_filter)
    db.recs_live.delete_many({"_id": {"$in": user_ids}})
    if not books:
        return
    remaining = {int(row["_id"]): row for row in db.user_ratings.aggregate([
        {"$match": {"book_id": {"$in": books}}},
        {"$group": {"_id": "$book_id", "rating_sum": {"$sum": "$rating"}, "n_ratings": {"$sum": 1}}},
    ])}
    operations = []
    for book_id in books:
        row = remaining.get(book_id)
        if row is None:
            operations.append(DeleteOne({"_id": book_id}))
        else:
            n_ratings = int(row["n_ratings"])
            operations.append(UpdateOne({"_id": book_id}, {"$set": {
                "rating_sum": int(row["rating_sum"]), "n_ratings": n_ratings,
                "avg_rating": float(row["rating_sum"]) / n_ratings}}))
    db.rating_stats.bulk_write(operations, ordered=False)
