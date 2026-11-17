"""Chuẩn hóa thể loại từ Goodreads, tùy chọn bù bằng Google Books và đồng bộ MongoDB."""
import argparse
import json
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pymongo import MongoClient, UpdateOne

from common.genres import GENRES, map_tags_to_genres
from common.settings import DATA_DIR, MONGO_DB, MONGO_URI, REPORT_DIR
from common.catalog import GoogleBooksUnavailable, google_books_metadata

log = logging.getLogger("bookpulse-genres")
DEFAULT_BOOKS = DATA_DIR / "goodbooks" / "books.csv"
DEFAULT_BOOK_TAGS = DATA_DIR / "goodbooks" / "book_tags.csv"
DEFAULT_TAGS = DATA_DIR / "goodbooks" / "tags.csv"
DEFAULT_OUTPUT = DATA_DIR / "goodbooks" / "books_with_genres.csv"
DEFAULT_GOOGLE_CACHE = DATA_DIR / "goodbooks" / "google_genre_cache.json"
ORIGINAL_TAG_LIMIT = 20
MIN_GENRE_TAG_COUNT = 5
MAX_GENRES_PER_BOOK = 3


def build_genre_records(books, book_tags, tags, google_lookup=None):
    """Ghép theo goodreads_book_id, giữ book_id nội bộ và tạo một document mỗi sách."""
    required = {
        "books": {"book_id", "goodreads_book_id", "title"},
        "book_tags": {"goodreads_book_id", "tag_id", "count"},
        "tags": {"tag_id", "tag_name"},
    }
    for name, frame in (("books", books), ("book_tags", book_tags), ("tags", tags)):
        missing = required[name] - set(frame.columns)
        if missing:
            raise ValueError(f"{name} thiếu cột bắt buộc: {', '.join(sorted(missing))}")

    books = books.copy()
    books["book_id"] = pd.to_numeric(books["book_id"], errors="coerce")
    books["goodreads_book_id"] = pd.to_numeric(books["goodreads_book_id"], errors="coerce")
    books = books.dropna(subset=["book_id", "goodreads_book_id"])
    duplicate_count = int(books["book_id"].duplicated().sum())
    books = books.drop_duplicates("book_id", keep="first")

    book_tags = book_tags.copy()
    book_tags["goodreads_book_id"] = pd.to_numeric(book_tags["goodreads_book_id"], errors="coerce")
    book_tags["tag_id"] = pd.to_numeric(book_tags["tag_id"], errors="coerce")
    book_tags["count"] = pd.to_numeric(book_tags["count"], errors="coerce").fillna(0)
    tags = tags.copy()
    tags["tag_id"] = pd.to_numeric(tags["tag_id"], errors="coerce")
    tag_names = tags.dropna(subset=["tag_id"]).drop_duplicates("tag_id")

    joined = (book_tags.dropna(subset=["goodreads_book_id", "tag_id"])
              .merge(tag_names[["tag_id", "tag_name"]], on="tag_id", how="inner")
              .merge(books[["book_id", "goodreads_book_id"]], on="goodreads_book_id", how="inner")
              .sort_values(["book_id", "count", "tag_name"], ascending=[True, False, True]))
    tags_by_book = {}
    for book_id, rows in joined.groupby("book_id", sort=False):
        names = [str(name) for name in rows["tag_name"].tolist() if pd.notna(name)]
        genre_votes = Counter()
        for row in rows.itertuples(index=False):
            tag_genres = map_tags_to_genres([row.tag_name])
            if row.count >= MIN_GENRE_TAG_COUNT:
                for genre in tag_genres:
                    genre_votes[genre] += int(row.count)
        ranked_genres = sorted(
            genre_votes,
            key=lambda genre: (-genre_votes[genre], GENRES.index(genre)),
        )
        tags_by_book[int(book_id)] = {
            "genres": ranked_genres[:MAX_GENRES_PER_BOOK],
            "original_tags": list(dict.fromkeys(names))[:ORIGINAL_TAG_LIMIT],
        }

    records = []
    for row in books.to_dict("records"):
        book_id = int(row["book_id"])
        evidence = tags_by_book.get(book_id, {"genres": [], "original_tags": []})
        genres = evidence["genres"]
        source = "goodreads_tags" if genres else "unknown"
        google_genres = []
        if not genres and google_lookup:
            google_genres = map_tags_to_genres(google_lookup(row))[:MAX_GENRES_PER_BOOK]
            if google_genres:
                genres = google_genres
                source = "google_books"
        records.append({
            **row,
            "book_id": book_id,
            "genres": genres or ["Unknown"],
            "original_tags": evidence["original_tags"],
            "genre_source": source,
        })
    return records, duplicate_count


def load_google_cache(path):
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Không đọc được cache Google Books {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Cache Google Books phải là JSON object: {path}")
    return payload


def make_google_lookup(cache_path, max_lookups):
    """Bổ sung có giới hạn; cache cả kết quả rỗng để tránh gọi lại liên tục."""
    cache = load_google_cache(cache_path)
    calls = 0
    blocked = False

    def lookup(book):
        nonlocal calls, blocked
        key = str(int(book["book_id"]))
        if key in cache:
            return cache[key]
        if blocked or calls >= max_lookups:
            return []
        calls += 1
        try:
            response = google_books_metadata({
                "book_id": key,
                "title": book.get("title"),
                "authors": book.get("authors"),
                "isbn": book.get("isbn"),
            })
            categories = response.get("genres", [])
        except GoogleBooksUnavailable as exc:
            log.warning("Google Books không tra được book_id=%s: %s", key, exc)
            blocked = True
            return []
        cache[key] = categories
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = cache_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
        temporary.replace(cache_path)
        time.sleep(0.1)
        return categories

    return lookup


def sync_mongo(records, batch_size=500):
    """Chỉ set các trường genre; không replace document và không đụng các trường hiện có."""
    client = MongoClient(MONGO_URI)
    try:
        collection = client[MONGO_DB].books
        updated_at = datetime.now(timezone.utc)
        matched = 0
        for offset in range(0, len(records), batch_size):
            batch = records[offset:offset + batch_size]
            operations = [
                UpdateOne(
                    {"_id": record["book_id"]},
                    {"$set": {
                        "genres": record["genres"],
                        "original_tags": record["original_tags"],
                        "genre_source": record["genre_source"],
                        "genre_updated_at": updated_at,
                    }},
                    upsert=True,
                )
                for record in batch
            ]
            result = collection.bulk_write(operations, ordered=False)
            matched += result.matched_count + result.upserted_count
        log.info("MongoDB đã cập nhật/đảm bảo %d sách", matched)
        return matched
    finally:
        client.close()


def summarize(records, duplicate_count, mongo_count=None):
    counts = Counter(genre for record in records for genre in record["genres"] if genre != "Unknown")
    unknown = sum(record["genres"] == ["Unknown"] for record in records)
    no_tags = sum(not record["original_tags"] for record in records)
    multi_genre = sum(len([g for g in r["genres"] if g != "Unknown"]) > 1 for r in records)
    return {
        "books_processed": len(records),
        "books_with_genre": len(records) - unknown,
        "books_unknown": unknown,
        "books_without_goodreads_tags": no_tags,
        "books_with_multiple_genres": multi_genre,
        "duplicate_book_ids_skipped": duplicate_count,
        "genre_sources": dict(Counter(record["genre_source"] for record in records)),
        "genre_counts": {genre: counts.get(genre, 0) for genre in GENRES},
        "mongo_documents_updated": mongo_count,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--books", type=Path, default=DEFAULT_BOOKS)
    parser.add_argument("--book-tags", type=Path, default=DEFAULT_BOOK_TAGS)
    parser.add_argument("--tags", type=Path, default=DEFAULT_TAGS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--google-books", action="store_true",
                        help="chỉ tra Google Books cho sách chưa phân loại")
    parser.add_argument("--google-limit", type=int, default=500,
                        help="tối đa số sách tra API mới trong một lần chạy")
    parser.add_argument("--google-cache", type=Path, default=DEFAULT_GOOGLE_CACHE)
    parser.add_argument("--csv-only", action="store_true", help="không cập nhật MongoDB")
    args = parser.parse_args()
    if args.google_limit < 0:
        parser.error("--google-limit không được âm")
    for path in (args.books, args.book_tags, args.tags):
        if not path.is_file():
            raise FileNotFoundError(f"Không tìm thấy file dữ liệu: {path}")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    books = pd.read_csv(args.books, low_memory=False)
    book_tags = pd.read_csv(args.book_tags, low_memory=False)
    tags = pd.read_csv(args.tags, low_memory=False)
    google_lookup = make_google_lookup(args.google_cache, args.google_limit) if args.google_books else None
    records, duplicates = build_genre_records(books, book_tags, tags, google_lookup)
    output = pd.DataFrame(records)
    output["genres"] = output["genres"].map(lambda values: json.dumps(values, ensure_ascii=False))
    output["original_tags"] = output["original_tags"].map(lambda values: json.dumps(values, ensure_ascii=False))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output, index=False, encoding="utf-8")
    log.info("CSV genre đã ghi: %s (%d dòng)", args.output, len(output))

    mongo_count = None if args.csv_only else sync_mongo(records)
    report = summarize(records, duplicates, mongo_count)
    report["google_books_enabled"] = args.google_books
    report["output_csv"] = str(args.output)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"book_genres_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"-> {report_path}")


if __name__ == "__main__":
    main()
