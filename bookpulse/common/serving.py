"""Helpers for building complete, catalog-backed recommendation shelves."""
import random


def complete_recommendations(recommendations, fallback_ids, popular_ids, catalog_ids, seen_ids, limit=10):
    """Keep the user's ranked results first, then fill a complete shelf with unseen catalog books."""
    catalog_ids = set(catalog_ids)
    seen_ids = set(seen_ids)
    result = []
    selected = set()

    def add(book_id, score=None):
        if book_id in catalog_ids and book_id not in seen_ids and book_id not in selected:
            result.append({"book_id": book_id, "score": score})
            selected.add(book_id)

    for recommendation in recommendations:
        add(recommendation["book_id"], recommendation.get("score"))
        if len(result) == limit:
            return result

    for book_id in [*fallback_ids, *popular_ids]:
        add(book_id)
        if len(result) == limit:
            return result

    return result


def cold_start_recommendations(catalog, seen_ids, seed, limit=10, pool_size=500):
    """Choose a stable, session-specific sample from the most popular unseen catalog books."""
    seen_ids = set(seen_ids)
    candidates = sorted(
        (book for book in catalog if book["book_id"] not in seen_ids),
        key=lambda book: (book.get("ratings_count", 0), book.get("average_rating", 0)),
        reverse=True,
    )[:pool_size]
    rng = random.Random(seed)
    selected = rng.sample(candidates, min(limit, len(candidates)))
    return [{"book_id": book["book_id"], "score": None} for book in selected]
