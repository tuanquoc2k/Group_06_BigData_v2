"""BookPulse reader page: recommendations and lightweight book discovery."""
import html
import json
import os
import secrets
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from common.events import make_producer, rating_event
from common.genres import GENRES, matches_genres
from common.serving import cold_start_recommendations, complete_recommendations
from common.settings import (
    ALS_MIN_RATING, FOLD_IN_MIN_RATINGS, DATA_DIR, KAFKA_TOPIC,
    RECOMMENDATION_TOP_K, get_db,
)

TOP_K = RECOMMENDATION_TOP_K
BOOKS_PER_PAGE = 12
RECOMMENDATIONS_PER_PAGE = 8
SPARK_UI_HOST = os.getenv("SPARK_UI_HOST", "spark")


@st.cache_resource
def db():
    return get_db()


@st.cache_resource
def producer():
    return make_producer()


@st.cache_data(ttl=10)
def stream_job_running():
    for port in (4040, 4041):
        try:
            with urllib.request.urlopen(
                f"http://{SPARK_UI_HOST}:{port}/api/v1/applications", timeout=0.5
            ) as response:
                if any(app.get("name") == "bookpulse-stream" for app in json.load(response)):
                    return True
        except (OSError, ValueError):
            continue
    return False


@st.cache_data(ttl=3600)
def load_catalog(path, modified):
    del modified
    frame = pd.read_csv(path, encoding="utf-8", encoding_errors="replace", low_memory=False)
    if "book_id" not in frame or "title" not in frame:
        raise ValueError(f"Catalog {path} must contain book_id and title columns")

    def field(name, default):
        return frame[name] if name in frame else default

    empty_text = pd.Series("", index=frame.index)
    empty_number = pd.Series(0, index=frame.index)
    catalog = pd.DataFrame({
        "book_id": pd.to_numeric(frame["book_id"], errors="coerce"),
        "title": field("title", empty_text).fillna("").astype(str),
        "authors": field("authors", pd.Series("Unknown author", index=frame.index))
        .fillna("Unknown author").astype(str),
        "year": pd.to_numeric(field("original_publication_year", empty_number), errors="coerce"),
        "average_rating": pd.to_numeric(field("average_rating", empty_number), errors="coerce"),
        "ratings_count": pd.to_numeric(field("ratings_count", empty_number), errors="coerce").fillna(0),
        "image_url": field("image_url", empty_text).fillna("").astype(str),
        "small_image_url": field("small_image_url", empty_text).fillna("").astype(str),
    }).dropna(subset=["book_id"])
    catalog["book_id"] = catalog["book_id"].astype(int)
    catalog["year"] = catalog["year"].fillna(0).astype(int)
    catalog["average_rating"] = catalog["average_rating"].fillna(0.0)
    if "genres" in frame:
        catalog["genres"] = frame.loc[catalog.index, "genres"].map(parse_genres)
    else:
        catalog["genres"] = [[] for _ in range(len(catalog))]
    return catalog.drop_duplicates("book_id").to_dict("records")


def parse_genres(value):
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return []
    return [str(item) for item in decoded if item] if isinstance(decoded, list) else []


@st.cache_data(ttl=300)
def load_mongo_genres():
    return {
        int(book["_id"]): parse_genres(book.get("genres"))
        for book in db().books.find({"genres": {"$exists": True}}, {"genres": 1})
    }


def latest_user_job(user_id):
    return db().recommendation_jobs.find_one(
        {"user_id": user_id}, sort=[("updated_at", -1)]
    )


def on_user_id_changed():
    st.session_state.pop("rating_submission", None)
    st.session_state.pop("pending_rating_events", None)
    st.session_state.pop("last_rating_result", None)
    st.session_state["recommendation_page"] = 0


def reset_recommendation_page():
    st.session_state["recommendation_page"] = 0


def publish_rating(user_id, book_id, stars, title):
    event = rating_event(user_id, book_id, stars)
    producer().send(KAFKA_TOPIC, key=user_id, value=event).get(timeout=10)
    submission = {
        "user_id": user_id,
        "book_id": book_id,
        "title": title,
        "rating": stars,
        "event_id": event["event_id"],
        "event_time": datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S.%f")
        .replace(tzinfo=timezone.utc),
        "sent_at": datetime.now(timezone.utc),
        "ingested": False,
    }
    pending = [
        row for row in st.session_state.get("pending_rating_events", [])
        if not (row.get("user_id") == user_id and row.get("book_id") == book_id)
    ]
    pending.append(submission)
    st.session_state["pending_rating_events"] = pending
    st.session_state.pop("last_rating_result", None)


def submit_feedback_rating(user_id, book_id, title, widget_key):
    score = st.session_state.get(widget_key)
    if score is None:
        return
    try:
        publish_rating(user_id, book_id, int(score) + 1, title)
    except Exception as exc:
        errors = st.session_state.setdefault("rating_send_errors", {})
        errors[book_id] = f"Không gửi được rating vào Kafka ({type(exc).__name__})."


def as_utc(value):
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def streamed_positive_ratings_since_snapshot(user_id, pending_events=None):
    database = db()
    meta = database.meta.find_one({"_id": "active"})
    snapshot = database.recs_live.find_one(
        {"_id": user_id, **({"model_version": meta["model_version"]} if meta else {})},
        {"updated_at": 1, "_id": 0},
    )
    baseline = as_utc(snapshot.get("updated_at")) if snapshot else None
    ratings_since_snapshot = {}
    for row in database.user_ratings.find(
        {"user_id": user_id},
        {"book_id": 1, "rating": 1, "event_time": 1, "ingested_at": 1, "_id": 0},
    ):
        ingested_at = as_utc(row.get("ingested_at") or row.get("event_time"))
        if baseline is None or (ingested_at is not None and ingested_at > baseline):
            ratings_since_snapshot[int(row["book_id"])] = int(row["rating"])
    for event in pending_events or []:
        if event.get("user_id") == user_id:
            ratings_since_snapshot[int(event["book_id"])] = int(event["rating"])
    return sum(rating >= ALS_MIN_RATING for rating in ratings_since_snapshot.values())


def rating_progress(user_id):
    """Return the real current state of this user's event/job, without a guessed percentage."""
    submission = st.session_state.get("rating_submission")
    if submission and submission.get("user_id") == user_id:
        rating_doc = db().user_ratings.find_one(
            {"user_id": user_id, "book_id": submission["book_id"]},
            {"_id": 0, "event_id": 1, "event_time": 1},
        )
        event_seen = bool(rating_doc and (
            rating_doc.get("event_id") == submission["event_id"]
            or (rating_doc.get("event_time")
                and as_utc(rating_doc["event_time"]) >= submission["event_time"])
        ))
        if not event_seen:
            if not stream_job_running():
                return "stopped", "Streaming đang dừng. Hãy bật job ở trang Điều khiển demo rồi tiếp tục theo dõi."
            return "ingesting", "Đang chờ Kafka và Spark ghi nhận lượt đánh giá."
        if not submission.get("ingested"):
            st.session_state["rating_submission"]["ingested"] = True
        if submission["rating"] < ALS_MIN_RATING:
            st.session_state["last_rating_result"] = {
                "kind": "hidden",
                "message": f"Đã lưu {'★' * submission['rating']}; sách được ẩn khỏi gợi ý. Rating thấp không đổi vector ALS.",
            }
            st.session_state.pop("rating_submission", None)
            return "done", "Đã ghi nhận sách đã xem."

        job = latest_user_job(user_id)
        event_time = submission["event_time"]
        job_covers_event = bool(job and (
            job.get("latest_event_id") == submission["event_id"]
            or (job.get("latest_event_time") and as_utc(job["latest_event_time"]) >= event_time)
        ))
        if job_covers_event and job.get("status") == "failed":
            st.session_state["last_rating_result"] = {
                "kind": "error", "message": "Rating đã tới MongoDB nhưng worker không cập nhật được. Kiểm tra log worker.",
            }
            st.session_state.pop("rating_submission", None)
            return "done", "Worker cập nhật thất bại."
        if job_covers_event and job.get("status") == "done":
            live = db().recs_live.find_one(
                {"_id": user_id},
                {"source": 1, "stream_positive_ratings": 1, "_id": 0},
            ) or {}
            if live.get("source") == "als-foldin" and job.get("recommendations_written", 0) > 0:
                result = {"kind": "success", "message": "Fold-in hoàn tất: gợi ý mới đã sẵn sàng."}
            elif job.get("result") == "waiting-for-min-ratings":
                positive_count = streamed_positive_ratings_since_snapshot(user_id)
                result = {
                    "kind": "info",
                    "message": (
                        f"Đã ghi nhận {positive_count}/{FOLD_IN_MIN_RATINGS} sách mới từ "
                        f"{ALS_MIN_RATING} sao kể từ lần fold-in gần nhất. Cần đủ "
                        f"{FOLD_IN_MIN_RATINGS} sách để chạy ALS fold-in."
                    ),
                }
            else:
                result = {"kind": "info", "message": "Rating đã lưu; chưa có vector cá nhân hợp lệ nên đang dùng gợi ý batch."}
            st.session_state["last_rating_result"] = result
            st.session_state.pop("rating_submission", None)
            return "done", result["message"]
        if job_covers_event and job.get("status") == "processing":
            return "folding", "Đang cập nhật sách phù hợp với sở thích của bạn."
        return "queued", "Đã nhận lượt đánh giá; đang chờ worker tạo gợi ý mới."

    job = latest_user_job(user_id)
    if job and job.get("status") in {"pending", "processing"}:
        if job["status"] == "processing":
            return "folding", "Đang cập nhật sách phù hợp với sở thích của bạn."
        return "queued", "Đã nhận sự kiện; đang chờ worker cập nhật gợi ý."
    return "done", ""


def show_recommendation_overlay(overlay, note):
    overlay.markdown(
        '<style>@keyframes bookpulse-spin { to { transform: rotate(360deg); } }</style>'
        '<div style="position:fixed;inset:0;z-index:1000000;display:flex;'
        'align-items:center;justify-content:center;padding:1rem;'
        'background:rgba(18,34,29,.62);backdrop-filter:blur(5px);'
        'pointer-events:all;">'
        '<div role="status" aria-live="polite" style="width:min(440px,92vw);'
        'padding:2rem;border-radius:20px;background:#fffdf8;text-align:center;'
        'box-shadow:0 24px 80px rgba(0,0,0,.28);color:#183c35;">'
        '<div style="width:42px;height:42px;margin:0 auto 1rem;border:4px solid #dce8df;'
        'border-top-color:#285d4f;border-radius:50%;animation:bookpulse-spin .8s linear infinite;"></div>'
        '<div style="font-size:1.15rem;font-weight:700;margin-bottom:.5rem;">'
        'Đang cập nhật sách phù hợp với sở thích của bạn</div>'
        f'<div style="color:#626c65;line-height:1.5;">{html.escape(note)}</div>'
        '<div style="margin-top:1rem;font-size:.82rem;color:#858b85;">'
        'Vui lòng chờ trong khi ALS fold-in làm mới gợi ý.</div></div></div>',
        unsafe_allow_html=True,
    )


def refresh_personal_books(user_id):
    """Read a fresh recommendation snapshot without rerunning the whole page."""
    database = db()
    meta = database.meta.find_one({"_id": "active"})
    if not meta:
        return []
    profile = database[meta["user_profiles_collection"]].find_one(
        {"_id": user_id}, {"seen_book_ids": 1, "positive_ratings": 1, "_id": 0}
    ) or {}
    seen_docs = list(database.user_seen.find(
        {"user_id": user_id}, {"book_id": 1, "rating": 1, "_id": 0}
    ))
    stream_ratings = list(database.user_ratings.find(
        {"user_id": user_id}, {"book_id": 1, "rating": 1, "_id": 0}
    ))
    seen = {int(book_id) for book_id in profile.get("seen_book_ids", [])}
    seen.update(int(row["book_id"]) for row in seen_docs)
    seen.update(int(row["book_id"]) for row in stream_ratings)
    pending_events = [
        row for row in st.session_state.get("pending_rating_events", [])
        if row.get("user_id") == user_id
    ]
    seen.update(int(row["book_id"]) for row in pending_events)
    ratings_by_book = {
        int(row["book_id"]): int(row["rating"])
        for row in profile.get("positive_ratings", [])
    }
    ratings_by_book.update({
        int(row["book_id"]): int(row["rating"])
        for row in seen_docs if row.get("rating") is not None
    })
    ratings_by_book.update({int(row["book_id"]): int(row["rating"]) for row in stream_ratings})
    ratings_by_book.update({int(row["book_id"]): int(row["rating"]) for row in pending_events})

    batch = database[meta["recs_collection"]].find_one({"_id": user_id})
    live = database.recs_live.find_one({
        "_id": user_id, "model_version": meta["model_version"], "stale": {"$ne": True}
    })
    active = live or batch
    popular_ids = [int(item["book_id"]) for item in meta.get("popular", [])]
    recommendations = complete_recommendations(
        (active or {}).get("recs", []),
        (batch or {}).get("fallback", popular_ids), popular_ids, by_id, seen, TOP_K,
    )
    if not recommendations:
        recommendations = cold_start_recommendations(catalog, seen, user_id, TOP_K)
    return [
        {**by_id[row["book_id"]], "user_rating": ratings_by_book.get(row["book_id"]),
         "score": row.get("score")}
        for row in recommendations if row["book_id"] in by_id
    ]


def refresh_history_books(user_id):
    """Refresh and sort the visible history cards after a rating without a page rerun."""
    database = db()
    meta = database.meta.find_one({"_id": "active"})
    profile = {}
    if meta and meta.get("user_profiles_collection"):
        profile = database[meta["user_profiles_collection"]].find_one(
            {"_id": user_id}, {"seen_book_ids": 1, "positive_ratings": 1, "_id": 0}
        ) or {}
    streams = list(database.user_ratings.find(
        {"user_id": user_id}, {"book_id": 1, "rating": 1, "event_time": 1, "_id": 0}
    ))
    seen_docs = list(database.user_seen.find(
        {"user_id": user_id}, {"book_id": 1, "rating": 1, "event_time": 1, "_id": 0}
    ))
    seen = {int(book_id) for book_id in profile.get("seen_book_ids", [])}
    seen.update(int(row["book_id"]) for row in seen_docs)
    seen.update(int(row["book_id"]) for row in streams)
    ratings = {int(row["book_id"]): int(row["rating"])
               for row in profile.get("positive_ratings", [])}
    ratings.update({
        int(row["book_id"]): int(row["rating"])
        for row in seen_docs if row.get("rating") is not None
    })
    ratings.update({int(row["book_id"]): int(row["rating"]) for row in streams})
    activity = {
        int(row["book_id"]): as_utc(row["event_time"])
        for row in seen_docs if row.get("event_time") is not None
    }
    activity.update({
        int(row["book_id"]): as_utc(row["event_time"])
        for row in streams if row.get("event_time") is not None
    })
    books = [
        {**by_id[book_id], "user_rating": ratings.get(book_id), "rated_at": activity.get(book_id)}
        for book_id in seen if book_id in by_id
    ]
    books.sort(
        key=lambda book: (
            book.get("rated_at") or datetime.min.replace(tzinfo=timezone.utc), book["book_id"]
        ),
        reverse=True,
    )
    return books[:BOOKS_PER_PAGE]


@st.fragment
def card_grid(books, key_prefix, user_id):
    """Keep rating and post-fold-in refreshes local to the current book shelf."""
    submission = st.session_state.get("rating_submission")
    if submission and submission.get("user_id") == user_id and submission.get("paused"):
        st.warning("Đang tạm dừng chờ xử lý. Bật streaming rồi tiếp tục theo dõi để cập nhật gợi ý.")
        if st.button("Tiếp tục theo dõi", type="primary", use_container_width=True):
            submission["paused"] = False
            st.rerun(scope="fragment")
        return

    pending_events = [
        row for row in st.session_state.get("pending_rating_events", [])
        if row.get("user_id") == user_id
    ]
    progress_count = streamed_positive_ratings_since_snapshot(user_id, pending_events)
    if (not submission or submission.get("user_id") != user_id) and pending_events \
            and progress_count >= FOLD_IN_MIN_RATINGS:
        submission = max(pending_events, key=lambda row: row.get("sent_at") or datetime.min.replace(tzinfo=timezone.utc))
        st.session_state["rating_submission"] = submission

    latest_job = latest_user_job(user_id)
    job_running = latest_job and latest_job.get("status") in {"pending", "processing"}
    submission_running = submission and submission.get("user_id") == user_id
    refresh_after_update = False
    fold_in_running = progress_count >= FOLD_IN_MIN_RATINGS and (submission_running or job_running)
    if fold_in_running:
        overlay = st.empty()
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            stage, note = rating_progress(user_id)
            if stage == "done":
                overlay.empty()
                refresh_after_update = True
                st.session_state.pop("pending_rating_events", None)
                break
            if stage == "stopped":
                overlay.empty()
                st.warning(note)
                if st.button("Đóng thông báo", type="primary", use_container_width=True):
                    if submission_running:
                        submission["paused"] = True
                    st.rerun(scope="fragment")
                return
            show_recommendation_overlay(overlay, note or "Đang xử lý lượt đánh giá của bạn.")
            time.sleep(1)
        if not refresh_after_update:
            overlay.empty()
            st.warning("Cập nhật đang lâu hơn dự kiến. Hãy chờ worker hoặc kiểm tra log trong Điều khiển demo.")
            if st.button("Tiếp tục chờ", type="primary", use_container_width=True):
                st.rerun(scope="fragment")
            return

    if progress_count < FOLD_IN_MIN_RATINGS and (progress_count or pending_events):
        st.info(f"Fold-in {progress_count}/{FOLD_IN_MIN_RATINGS} · chấm thêm sách từ {ALS_MIN_RATING}★.")
    elif refresh_after_update:
        next_count = streamed_positive_ratings_since_snapshot(user_id)
        st.caption(f"Đã cập nhật gợi ý · lần tiếp theo {next_count}/{FOLD_IN_MIN_RATINGS}.")

    result = st.session_state.pop("last_rating_result", None)
    if result:
        if result["kind"] == "success":
            st.success(result["message"])
        elif result["kind"] == "error":
            st.error(result["message"])
        else:
            st.info(result["message"])
    if key_prefix == "personal" and (result or refresh_after_update):
        books = refresh_personal_books(user_id)
    elif key_prefix == "history" and (result or refresh_after_update):
        books = refresh_history_books(user_id)

    pending_by_book = {
        int(row["book_id"]): row for row in pending_events
    }
    for book in books:
        pending = pending_by_book.get(int(book["book_id"]))
        if pending:
            book["user_rating"] = int(pending["rating"])
            book["rated_at"] = pending.get("event_time")
    if key_prefix == "personal":
        persisted_seen = {
            int(row["book_id"])
            for row in db().user_seen.find({"user_id": user_id}, {"book_id": 1, "_id": 0})
        }
        persisted_seen.update(
            int(row["book_id"])
            for row in db().user_ratings.find({"user_id": user_id}, {"book_id": 1, "_id": 0})
        )
        persisted_seen.update(pending_by_book)
        books = [book for book in books if int(book["book_id"]) not in persisted_seen]

    recommendation_page = 0
    recommendation_pages = 1
    if key_prefix == "personal":
        available_genres = sorted({
            genre for book in books for genre in book.get("genres", [])
        })
        filter_column, sort_column = st.columns([1.6, 1])
        selected_genres = filter_column.multiselect(
            "Lọc thể loại", available_genres,
            key=f"recommendation_genres-{user_id}",
            on_change=reset_recommendation_page,
            placeholder="Tất cả thể loại",
        )
        sort_method = sort_column.selectbox(
            "Sắp xếp sách",
            ["Điểm ALS", "Thể loại", "Đánh giá độc giả", "Độ phổ biến"],
            key=f"recommendation_sort-{user_id}",
            on_change=reset_recommendation_page,
        )
        books = [book for book in books if matches_genres(book.get("genres"), selected_genres)]
        if sort_method == "Điểm ALS":
            books.sort(key=lambda book: book.get("score") or 0, reverse=True)
        elif sort_method == "Thể loại":
            books.sort(key=lambda book: (" · ".join(sorted(book.get("genres", []))).casefold(),
                                         -(book.get("score") or 0)))
        elif sort_method == "Đánh giá độc giả":
            books.sort(key=lambda book: (book.get("average_rating") or 0,
                                         book.get("ratings_count") or 0), reverse=True)
        else:
            books.sort(key=lambda book: (book.get("ratings_count") or 0,
                                         book.get("average_rating") or 0), reverse=True)
        recommendation_pages = max(
            1, (len(books) + RECOMMENDATIONS_PER_PAGE - 1) // RECOMMENDATIONS_PER_PAGE
        )
        recommendation_page = min(
            max(0, int(st.session_state.get("recommendation_page", 0))),
            recommendation_pages - 1,
        )
        st.session_state["recommendation_page"] = recommendation_page
        start = recommendation_page * RECOMMENDATIONS_PER_PAGE
        books = books[start:start + RECOMMENDATIONS_PER_PAGE]
        st.caption(
            f"Trang {recommendation_page + 1}/{recommendation_pages} · "
            f"{RECOMMENDATIONS_PER_PAGE} sách mỗi trang · {sort_method.casefold()}"
        )

    if not books:
        st.info("Không tìm thấy sách phù hợp.")
        return
    for start in range(0, len(books), 4):
        columns = st.columns(4, gap="medium")
        for column, book in zip(columns, books[start:start + 4]):
            with column:
                with st.container(border=True):
                    cover = book.get("image_url") or book.get("small_image_url")
                    if cover:
                        st.image(cover, use_column_width=True)
                    else:
                        st.markdown('<div class="cover-empty">📖</div>', unsafe_allow_html=True)
                    title = html.escape(book["title"])
                    authors = html.escape(book["authors"])
                    genres = "".join(
                        f'<span class="book-tag">{html.escape(genre)}</span>'
                        for genre in book.get("genres", [])
                    )
                    genre_markup = (
                        f'<div class="book-genres">{genres or "<span class=genre-empty>Chưa có thể loại</span>"}</div>'
                    )
                    st.markdown(
                        f'<div class="book-title">{title}</div>'
                        f'<div class="book-author">{authors}</div>{genre_markup}',
                        unsafe_allow_html=True,
                    )
                    rating = float(book.get("average_rating") or 0)
                    if rating:
                        full_stars = max(1, min(5, int(round(rating))))
                        st.markdown(
                            f'<div class="book-average">Độc giả · {"★" * full_stars}{"☆" * (5 - full_stars)}</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown('<div class="book-average">&nbsp;</div>', unsafe_allow_html=True)
                    if book.get("user_rating") is not None:
                        mine = "★" * int(book["user_rating"]) + "☆" * (5 - int(book["user_rating"]))
                        st.markdown(f'<div class="book-user-rating">Bạn · {mine}</div>', unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="book-user-rating">&nbsp;</div>', unsafe_allow_html=True)
                    widget_key = f"book-feedback-{user_id}-{int(book['book_id'])}"
                    saved_rating = book.get("user_rating")
                    if saved_rating is None:
                        st.session_state.pop(widget_key, None)
                    else:
                        st.session_state[widget_key] = int(saved_rating) - 1
                    st.feedback(
                        "stars", key=widget_key,
                        on_change=submit_feedback_rating,
                        args=(user_id, int(book["book_id"]), book["title"], widget_key),
                        disabled=bool(st.session_state.get("rating_submission")),
                    )
                    errors = st.session_state.get("rating_send_errors", {})
                    error = errors.pop(int(book["book_id"]), None)
                    if error:
                        st.error(error)
    if key_prefix == "personal":
        previous_column, next_column = st.columns(2)
        if previous_column.button(
            "← Trang trước", disabled=recommendation_page == 0,
            use_container_width=True, key="recommendations_previous",
        ):
            st.session_state["recommendation_page"] = recommendation_page - 1
            st.rerun(scope="fragment")
        if next_column.button(
            "Trang sau →", disabled=recommendation_page + 1 >= recommendation_pages,
            use_container_width=True, key="recommendations_next",
        ):
            st.session_state["recommendation_page"] = recommendation_page + 1
            st.rerun(scope="fragment")


st.markdown("""
<style>
    .stApp { background: #f7f5f0; }
    [data-testid="stHeader"] { background: rgba(247,245,240,.92); }
    .block-container { max-width: 1220px; padding: 2rem 2.4rem 3.5rem; }
    .hero { padding: 2rem 2.4rem; border-radius: 22px; color: #fff;
        background: linear-gradient(115deg,#183c35 0%,#285d4f 62%,#c47b4d 140%); margin-bottom: .7rem; }
    .hero-kicker { color: #d3e4c9; letter-spacing: .12em; text-transform: uppercase;
        font-size: .72rem; font-weight: 700; }
    .hero h1 { color: #fff; font-size: clamp(1.8rem,3.4vw,2.7rem); margin: .3rem 0; }
    .hero p { color: #e5eee7; font-size: 1rem; margin: 0; }
                    .book-title { font-weight: 700; line-height: 1.3; height: 2.65em; margin-top: .45rem;
                        overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; }
    .book-author { color: #6b716d; font-size: .86rem; height: 2.4em; overflow: hidden; }
    .book-genres { display: flex; flex-wrap: wrap; align-content: flex-start; gap: .3rem;
        height: 2.8rem; overflow: hidden; }
    .book-average, .book-user-rating { height: 1.35rem; line-height: 1.2rem; font-size: .8rem; color: #606b63; }
    .genre-empty { color: #999; font-size: .72rem; }
    .book-tag { background: #e5eee7; color: #285d4f; border-radius: 999px;
        padding: .15rem .5rem; font-size: .7rem; }
    [data-testid="stImage"] img { width: 100%; height: 210px; object-fit: contain;
        background: #fff; border-radius: 8px; }
    .cover-empty { height: 210px; display: grid; place-items: center;
        background: #e8e4dc; border-radius: 10px; font-size: 2.5rem; }
    [data-testid="stRadio"] > div { gap: .5rem; }
    [data-testid="stRadio"] label { border-radius: 999px; padding: .15rem .75rem; }
    [data-testid="stVerticalBlockBorderWrapper"] { border-color: #e7e3da; }
    [data-testid="stButton"] button { border-radius: 10px; }
    [data-testid="stFeedback"] { padding: .1rem 0 .25rem; }
    @media (max-width: 720px) {
        .block-container { padding: .8rem .8rem 2rem; }
        [data-testid="stImage"] img, .cover-empty { height: 170px; }
        .hero { padding: 1.25rem; }
    }
</style>
""", unsafe_allow_html=True)

if "selected_user_id" not in st.session_state:
    st.session_state["selected_user_id"] = 1
if "browse_page" not in st.session_state:
    st.session_state["browse_page"] = 0
if "recommendation_page" not in st.session_state:
    st.session_state["recommendation_page"] = 0

with st.sidebar:
    st.markdown("## 📚 BookPulse")
    st.number_input("Mã người đọc", min_value=1, step=1, key="selected_user_id", on_change=on_user_id_changed)
    user_id = int(st.session_state["selected_user_id"])
    if st.button("Tạo hồ sơ khách mới", use_container_width=True):
        st.session_state["selected_user_id"] = secrets.randbelow(900_000_000) + 100_000_000
        st.rerun()
    if stream_job_running():
        st.success("Streaming đang hoạt động")
    else:
        st.warning("Streaming đang tạm dừng")
        st.caption("Có thể bật job ở trang Điều khiển demo.")

catalog_path = next((path for path in (
    DATA_DIR / "goodbooks" / "books_with_genres.csv",
    DATA_DIR / "goodbooks" / "books.csv",
    DATA_DIR / "sample" / "books.csv",
) if path.is_file()), None)
if catalog_path:
    catalog = load_catalog(str(catalog_path), catalog_path.stat().st_mtime)
else:
    catalog = [{
        "book_id": book["_id"], "title": book.get("title", "—"),
        "authors": book.get("authors", "—"), "year": 0, "average_rating": 0,
        "ratings_count": 0, "image_url": "", "small_image_url": "", "genres": [],
    } for book in db().books.find({}, {"title": 1, "authors": 1})]
if not catalog:
    st.error("Chưa có danh mục sách. Hãy tải Goodbooks-10k hoặc tạo dữ liệu mẫu trước.")
    st.stop()

stored_genres = load_mongo_genres()
for book in catalog:
    book["genres"] = stored_genres.get(book["book_id"], book.get("genres", []))
by_id = {book["book_id"]: book for book in catalog}
database = db()
meta = database.meta.find_one({"_id": "active"})
profile = {}
if meta and meta.get("user_profiles_collection"):
    profile = database[meta["user_profiles_collection"]].find_one(
        {"_id": user_id}, {"seen_book_ids": 1, "positive_ratings": 1, "_id": 0}
    ) or {}
stream_ratings = list(database.user_ratings.find(
    {"user_id": user_id},
    {"book_id": 1, "rating": 1, "event_time": 1, "ingested_at": 1, "_id": 0},
))
user_seen_docs = list(database.user_seen.find(
    {"user_id": user_id}, {"book_id": 1, "rating": 1, "event_time": 1, "_id": 0}
))
pending_events = [
    row for row in st.session_state.get("pending_rating_events", [])
    if row.get("user_id") == user_id
]
user_seen_ids = {int(doc["book_id"]) for doc in user_seen_docs}
seen = user_seen_ids | {int(book_id) for book_id in profile.get("seen_book_ids", [])}
seen.update(int(row["book_id"]) for row in stream_ratings)
seen.update(int(row["book_id"]) for row in pending_events)
activity_by_book = {
    int(doc["book_id"]): as_utc(doc.get("event_time"))
    for doc in user_seen_docs if doc.get("event_time") is not None
}
activity_by_book.update({
    int(row["book_id"]): as_utc(row.get("event_time"))
    for row in stream_ratings if row.get("event_time") is not None
})
activity_by_book.update({
    int(row["book_id"]): as_utc(row.get("event_time"))
    for row in pending_events if row.get("event_time") is not None
})

ratings_by_book = {
    int(item["book_id"]): int(item["rating"])
    for item in profile.get("positive_ratings", [])
}
for row in user_seen_docs:
    if row.get("rating") is not None:
        ratings_by_book[int(row["book_id"])] = int(row["rating"])
for row in stream_ratings:
    ratings_by_book[int(row["book_id"])] = int(row["rating"])
for row in pending_events:
    ratings_by_book[int(row["book_id"])] = int(row["rating"])
rated_books = [
    {**by_id[book_id], "user_rating": ratings_by_book.get(book_id),
     "rated_at": activity_by_book.get(book_id)}
    for book_id in seen if book_id in by_id
]
rated_books.sort(
    key=lambda book: (book.get("rated_at") or datetime.min.replace(tzinfo=timezone.utc), book["book_id"]),
    reverse=True,
)

recommendations = []
recommendation_source = "Khám phá phổ biến · hồ sơ khách mới"
recommendation_updated_at = None
hidden = 0
filled = 0
live_doc = None
batch_doc = None
latest_job = latest_user_job(user_id)
if meta:
    batch_doc = database[meta["recs_collection"]].find_one({"_id": user_id})
    live_doc = database.recs_live.find_one({
        "_id": user_id, "model_version": meta["model_version"], "stale": {"$ne": True}
    })
    active_doc = live_doc or batch_doc
    if active_doc:
        hidden = sum(rec["book_id"] in seen for rec in active_doc.get("recs", []))
        recommendation_source = (
            f"ALS fold-in · model {meta['model_version']}"
            if live_doc and live_doc.get("source") == "als-foldin"
            else f"Gợi ý batch · model {meta['model_version']}"
        )
        recommendation_updated_at = (
            live_doc.get("updated_at") if live_doc
            else batch_doc.get("created_at", meta.get("switched_at"))
        )
    popular = meta.get("popular", [])
    popular_ids = [int(item["book_id"]) for item in popular]
    fallback = (batch_doc or {}).get("fallback", popular_ids)
    if active_doc and active_doc.get("recs"):
        recommendations = complete_recommendations(
            active_doc["recs"], fallback, popular_ids, by_id, seen, TOP_K,
        )
        filled = sum(rec.get("score") is None for rec in recommendations)
    if not recommendations and popular_ids:
        recommendations = complete_recommendations([], [], popular_ids, by_id, seen, TOP_K)
        recommendation_source = f"Sách phổ biến · model {meta['model_version']}"
        recommendation_updated_at = meta.get("switched_at")
if not recommendations:
    recommendations = cold_start_recommendations(catalog, seen, user_id, TOP_K)
    recommendation_source = "Khách mới · khám phá phổ biến theo phiên"

snapshot_at = as_utc(live_doc.get("updated_at")) if live_doc else None
stream_positive_count = sum(
    row["rating"] >= ALS_MIN_RATING
    and (snapshot_at is None or
         (as_utc(row.get("ingested_at") or row.get("event_time")) is not None
          and as_utc(row.get("ingested_at") or row.get("event_time")) > snapshot_at))
    for row in stream_ratings
)

st.markdown("""
<section class="hero">
  <div class="hero-kicker">BookPulse · khám phá theo nhịp đọc</div>
  <h1>Mỗi cuốn sách mở ra một nhịp mới.</h1>
  <p>Tìm sách, xem gợi ý và thử ALS fold-in bằng cách chấm sao ngay dưới mỗi cuốn.</p>
</section>
""", unsafe_allow_html=True)
toolbar, refresh_column = st.columns([5, 1])
toolbar.caption(
    f"Hồ sơ #{user_id} · fold-in {min(stream_positive_count, FOLD_IN_MIN_RATINGS)}/{FOLD_IN_MIN_RATINGS}"
)
if refresh_column.button("Làm mới", use_container_width=True):
    st.rerun()

view = st.radio(
    "Nội dung", ["Gợi ý cho bạn", "Khám phá sách", "Sách đã xem"],
    horizontal=True, label_visibility="collapsed",
)

if view == "Gợi ý cho bạn":
    st.subheader("Gợi ý dành riêng cho bạn")
    details = [recommendation_source, f"{len(recommendations[:TOP_K])} gợi ý"]
    if filled:
        details.append(f"{filled} sách phổ biến bổ sung")
    if recommendation_updated_at:
        updated = recommendation_updated_at
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        details.append(f"cập nhật {updated.astimezone(timezone(timedelta(hours=7))).strftime('%H:%M %d/%m/%Y')}")
    st.caption(" · ".join(details))
    if latest_job and latest_job.get("status") == "failed":
        st.error("Lần cập nhật gần nhất thất bại. Kiểm tra log recommendation-worker.")
    elif live_doc and live_doc.get("source") == "als-foldin":
        st.success("Đã cập nhật gợi ý theo lượt chấm mới nhất (ALS fold-in).")
    elif stream_positive_count < FOLD_IN_MIN_RATINGS:
        st.info(f"Chấm từ {ALS_MIN_RATING}★ để cập nhật gợi ý · dưới {ALS_MIN_RATING}★ chỉ ẩn sách đã xem.")
    elif live_doc or (latest_job and latest_job.get("status") == "done"
                      and not latest_job.get("recommendations_written")):
        st.info("Chưa tính được gu cá nhân, đang dùng gợi ý batch.")
    rec_books = [
        {**by_id[rec["book_id"]], "user_rating": ratings_by_book.get(rec["book_id"]),
         "score": rec.get("score")}
        for rec in recommendations[:TOP_K] if rec["book_id"] in by_id
    ]
    card_grid(rec_books, "personal", user_id)

elif view == "Khám phá sách":
    st.subheader("Khám phá danh mục")
    available_genres = [genre for genre in GENRES if any(genre in book.get("genres", []) for book in catalog)]
    with st.form("catalog_filters"):
        search_col, shelf_col, genre_col = st.columns([2.5, 1.2, 2.3])
        query = search_col.text_input("Tìm tên sách hoặc tác giả", key="browse_search").strip().casefold()
        shelf = shelf_col.selectbox("Sắp xếp", ["Phổ biến", "Đánh giá cao", "Nhiều độc giả", "Kinh điển"])
        selected_genres = genre_col.multiselect("Thể loại", available_genres, key="browse_genres")
        apply_filters = st.form_submit_button("Tìm sách", type="primary")
    if apply_filters:
        st.session_state["browse_page"] = 0
    filtered = [
        book for book in catalog
        if (not query or query in book["title"].casefold() or query in book["authors"].casefold())
        and (shelf != "Đánh giá cao" or book["average_rating"] >= 4.2)
        and (shelf != "Nhiều độc giả" or book["ratings_count"] >= 100_000)
        and (shelf != "Kinh điển" or 0 < book["year"] < 2000)
        and matches_genres(book.get("genres"), selected_genres)
    ]
    filtered.sort(key=lambda book: (book["ratings_count"], book["average_rating"]), reverse=True)
    total_pages = max(1, (len(filtered) + BOOKS_PER_PAGE - 1) // BOOKS_PER_PAGE)
    st.session_state["browse_page"] = min(st.session_state["browse_page"], total_pages - 1)
    page = st.session_state["browse_page"]
    start = page * BOOKS_PER_PAGE
    st.caption(f"{len(filtered):,} sách phù hợp · trang {page + 1}/{total_pages}")
    browse_cards = [
        {**book, "user_rating": ratings_by_book.get(book["book_id"])}
        for book in filtered[start:start + BOOKS_PER_PAGE]
    ]
    card_grid(browse_cards, "browse", user_id)
    prev_col, next_col = st.columns(2)
    if prev_col.button("← Trang trước", disabled=page == 0, use_container_width=True):
        st.session_state["browse_page"] -= 1
        st.rerun()
    if next_col.button("Trang sau →", disabled=page + 1 >= total_pages, use_container_width=True):
        st.session_state["browse_page"] += 1
        st.rerun()

else:
    st.subheader("Sách đã xem trong hồ sơ")
    st.caption(f"Đã ghi nhận {len(seen):,} sách đã tương tác cho người đọc #{user_id}.")
    st.caption(f"Hiển thị tối đa 12 trong {len(rated_books):,} sách.")
    card_grid(rated_books[:BOOKS_PER_PAGE], "history", user_id)
