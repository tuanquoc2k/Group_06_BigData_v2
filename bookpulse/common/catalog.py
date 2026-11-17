"""Book metadata helpers for the Streamlit catalog."""
import json
import os
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


_BROAD_CATEGORIES = {
    "fiction",
    "juvenile fiction",
    "young adult fiction",
    "nonfiction",
    "juvenile nonfiction",
    "biography & autobiography",
    "comics & graphic novels",
}
_rate_limit_lock = threading.Lock()
_rate_limited_until = 0


class GoogleBooksUnavailable(RuntimeError):
    """Google Books lookup failed, so callers may show local cover data and retry later."""


def google_books_metadata(book):
    """Look up a cover and genre categories through the public Google Books API."""
    global _rate_limited_until

    isbn = str(book.get("isbn") or "").strip()
    title = str(book.get("title") or "").strip()
    authors = str(book.get("authors") or "").strip()
    if isbn and isbn.lower() != "nan":
        query = f"isbn:{isbn}"
    elif title:
        query = f'intitle:"{title}"'
        if authors:
            query += f' inauthor:"{authors.split(",")[0]}"'
    else:
        return {"cover": None, "genres": [], "available": False}

    url = "https://www.googleapis.com/books/v1/volumes?" + urlencode({
        "q": query,
        "maxResults": 1,
        "fields": "items(volumeInfo(categories,imageLinks/thumbnail))",
    })
    api_key = os.getenv("GOOGLE_BOOKS_API_KEY")
    if api_key:
        url += "&" + urlencode({"key": api_key})
    request = Request(url, headers={"User-Agent": "BookPulse/1.0"})
    with _rate_limit_lock:
        if time.monotonic() < _rate_limited_until:
            raise GoogleBooksUnavailable("Google Books rate limit is temporarily active")
    payload = None
    for attempt in range(3):
        try:
            with urlopen(request, timeout=5) as response:
                payload = json.load(response)
            break
        except HTTPError as exc:
            if exc.code == 429:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                try:
                    cooldown = min(max(float(retry_after), 30), 300) if retry_after else 60
                except ValueError:
                    cooldown = 60
                with _rate_limit_lock:
                    _rate_limited_until = time.monotonic() + cooldown
                raise GoogleBooksUnavailable("Google Books rate limit was reached") from exc
            if exc.code < 500:
                raise GoogleBooksUnavailable(f"Google Books returned HTTP {exc.code}") from exc
            if attempt == 2:
                raise GoogleBooksUnavailable(f"Google Books returned HTTP {exc.code}") from exc
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            try:
                delay = min(max(float(retry_after), 1), 5) if retry_after else 2 ** attempt
            except ValueError:
                delay = 2 ** attempt
            time.sleep(delay)
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt == 2:
                raise GoogleBooksUnavailable("Google Books is unavailable") from exc
            time.sleep(2 ** attempt)

    items = payload.get("items") or []
    if not items:
        return {"cover": None, "genres": [], "available": False}
    info = items[0].get("volumeInfo") or {}
    image = (info.get("imageLinks") or {}).get("thumbnail")
    if image:
        image = image.replace("http://", "https://", 1)
    categories = info.get("categories") or []
    genres = []
    for category in categories:
        parts = [value.strip() for value in category.split("/") if value.strip()]
        if len(parts) > 1 and parts[0].casefold() in _BROAD_CATEGORIES:
            parts = parts[1:]
        for value in parts:
            if value and value not in genres:
                genres.append(value)
    return {"cover": image, "genres": genres[:3], "available": True}


def book_genres(google_genres):
    """Only show genres returned by Google Books; do not infer them from ratings."""
    return google_genres[:3]
