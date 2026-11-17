"""Tests for Google Books cover and genre enrichment."""
import json
import unittest
from email.message import Message
from io import BytesIO
from urllib.error import HTTPError
from unittest.mock import patch

import common.catalog as catalog
from common.catalog import GoogleBooksUnavailable, book_genres, google_books_metadata


class GoogleBooksGenreTest(unittest.TestCase):
    @patch("common.catalog.urlopen")
    def test_extracts_google_cover_and_genres(self, open_url):
        response = BytesIO(json.dumps({
            "items": [{
                "volumeInfo": {
                    "categories": ["Fiction / Fantasy / Epic", "Young Adult Fiction"],
                    "imageLinks": {"thumbnail": "http://books.google.com/cover.jpg"},
                },
            }],
        }).encode("utf-8"))
        open_url.return_value.__enter__.return_value = response
        result = google_books_metadata({
            "isbn": "1234567890", "title": "Sample", "authors": "A. Author",
        })
        self.assertEqual(result, {
            "cover": "https://books.google.com/cover.jpg",
            "genres": ["Fantasy", "Epic", "Young Adult Fiction"],
            "available": True,
        })
        request = open_url.call_args.args[0]
        self.assertIn("isbn%3A1234567890", request.full_url)

    @patch("common.catalog.time.sleep")
    @patch("common.catalog.urlopen", side_effect=TimeoutError)
    def test_google_timeout_does_not_invent_genres(self, _open_url, _sleep):
        with self.assertRaises(GoogleBooksUnavailable):
            google_books_metadata({"isbn": "123", "title": "Book"})
        self.assertEqual(book_genres([]), [])

    @patch("common.catalog.time.monotonic", return_value=100)
    @patch("common.catalog.urlopen")
    def test_google_rate_limit_opens_a_circuit_for_other_books(self, open_url, _clock):
        catalog._rate_limited_until = 0
        error = HTTPError(
            "https://www.googleapis.com/books/v1/volumes", 429, "limited", Message(), None,
        )
        open_url.side_effect = error
        with self.assertRaises(GoogleBooksUnavailable):
            google_books_metadata({"isbn": "123"})
        with self.assertRaises(GoogleBooksUnavailable):
            google_books_metadata({"isbn": "456"})
        open_url.assert_called_once()
        catalog._rate_limited_until = 0


if __name__ == "__main__":
    unittest.main()
