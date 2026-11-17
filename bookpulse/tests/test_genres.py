"""Kiểm thử mapping tag Goodreads và pipeline tạo catalog thể loại."""
import unittest

import pandas as pd

from common.genres import map_tags_to_genres, matches_genres
from scripts.update_book_genres import build_genre_records, summarize


class BookGenresTest(unittest.TestCase):
    def test_maps_only_supported_genre_evidence_and_allows_multiple_genres(self):
        genres = map_tags_to_genres([
            "urban-fantasy", "young-adult", "to-read", "books-i-own", "fantasy",
        ])
        self.assertEqual(genres, ["Fantasy", "Young Adult"])

    def test_generic_fiction_and_popularity_tags_do_not_invent_genres(self):
        self.assertEqual(map_tags_to_genres(["fiction", "to-read", "favorites"]), [])

    def test_selected_genres_filter_as_or_and_empty_selection_keeps_all(self):
        self.assertTrue(matches_genres(["Fantasy", "Adventure"], ["Mystery", "Fantasy"]))
        self.assertFalse(matches_genres(["Fantasy"], ["Mystery"]))
        self.assertTrue(matches_genres([], []))

    def test_build_joins_goodreads_id_preserves_book_id_and_unknown_books(self):
        books = pd.DataFrame([
            {"book_id": 41, "goodreads_book_id": 9001, "title": "Fantasy book", "authors": "A"},
            {"book_id": 42, "goodreads_book_id": 9002, "title": "No tags", "authors": "B"},
        ])
        book_tags = pd.DataFrame([
            {"goodreads_book_id": 9001, "tag_id": 10, "count": 100},
            {"goodreads_book_id": 9001, "tag_id": 11, "count": 90},
            {"goodreads_book_id": 9001, "tag_id": 12, "count": 80},
        ])
        tags = pd.DataFrame([
            {"tag_id": 10, "tag_name": "young-adult"},
            {"tag_id": 11, "tag_name": "fantasy"},
            {"tag_id": 12, "tag_name": "to-read"},
        ])

        records, duplicates = build_genre_records(books, book_tags, tags)

        self.assertEqual(duplicates, 0)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["book_id"], 41)
        self.assertEqual(records[0]["genres"], ["Young Adult", "Fantasy"])
        self.assertEqual(records[1]["genres"], ["Unknown"])
        report = summarize(records, duplicates)
        self.assertEqual(report["books_processed"], 2)
        self.assertEqual(report["books_unknown"], 1)
        self.assertEqual(report["genre_counts"]["Fantasy"], 1)

    def test_duplicate_internal_book_ids_produce_one_record(self):
        books = pd.DataFrame([
            {"book_id": 41, "goodreads_book_id": 9001, "title": "First"},
            {"book_id": 41, "goodreads_book_id": 9002, "title": "Duplicate"},
        ])
        book_tags = pd.DataFrame(columns=["goodreads_book_id", "tag_id", "count"])
        tags = pd.DataFrame(columns=["tag_id", "tag_name"])
        records, duplicates = build_genre_records(books, book_tags, tags)
        self.assertEqual(len(records), 1)
        self.assertEqual(duplicates, 1)
        self.assertEqual(records[0]["title"], "First")

    def test_google_categories_are_only_used_for_unclassified_books(self):
        books = pd.DataFrame([
            {"book_id": 1, "goodreads_book_id": 10, "title": "Has tag"},
            {"book_id": 2, "goodreads_book_id": 20, "title": "Needs fallback"},
        ])
        book_tags = pd.DataFrame([{"goodreads_book_id": 10, "tag_id": 1, "count": 20}])
        tags = pd.DataFrame([{"tag_id": 1, "tag_name": "fantasy"}])
        lookups = []

        def google_lookup(book):
            lookups.append(book["book_id"])
            return ["Fiction / Mystery"]

        records, _ = build_genre_records(books, book_tags, tags, google_lookup)
        self.assertEqual(lookups, [2])
        self.assertEqual(records[0]["genre_source"], "goodreads_tags")
        self.assertEqual(records[1]["genres"], ["Mystery"])
        self.assertEqual(records[1]["genre_source"], "google_books")


if __name__ == "__main__":
    unittest.main()
