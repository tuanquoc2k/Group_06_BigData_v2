"""Chuẩn hóa các tag có căn cứ thành bộ thể loại thống nhất."""
import re


GENRES = (
    "Fantasy",
    "Romance",
    "Mystery",
    "Thriller",
    "Science Fiction",
    "Horror",
    "Historical Fiction",
    "Contemporary Fiction",
    "Young Adult",
    "Adventure",
    "Biography",
    "History",
    "Self-Help",
    "Philosophy",
    "Poetry",
    "Non-Fiction",
    "Children",
)

_ALIASES = {
    "Fantasy": ("fantasy", "magical realism", "urban fantasy", "epic fantasy", "high fantasy"),
    "Romance": ("romance", "romantic fiction", "romantic suspense"),
    "Mystery": ("mystery", "mysteries", "detective fiction", "crime fiction"),
    "Thriller": ("thriller", "thrillers", "suspense", "psychological thriller"),
    "Science Fiction": ("science fiction", "sci fi", "scifi", "space opera", "dystopian"),
    "Horror": ("horror", "ghost stories", "gothic fiction"),
    "Historical Fiction": ("historical fiction", "historical novel", "historical novels"),
    "Contemporary Fiction": ("contemporary fiction", "contemporary literature"),
    "Young Adult": ("young adult", "ya fiction", "ya books", "teen fiction", "teen books"),
    "Adventure": ("adventure", "adventure fiction"),
    "Biography": ("biography", "autobiography", "memoir", "memoirs", "biographies"),
    "History": ("history", "historical non fiction", "world history"),
    "Self-Help": ("self help", "self improvement", "personal development"),
    "Philosophy": ("philosophy", "philosophical fiction"),
    "Poetry": ("poetry", "poems", "poetry collections"),
    "Non-Fiction": ("non fiction", "nonfiction", "non fiction books"),
    "Children": ("children", "childrens", "children s books", "juvenile fiction", "juvenile literature"),
}
def normalize_tag(value):
    """Chuẩn hóa dấu câu để tag Goodreads và category Google dùng cùng quy tắc."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value).casefold())).strip()


_NORMALIZED_ALIASES = {
    genre: tuple(normalize_tag(alias) for alias in aliases)
    for genre, aliases in _ALIASES.items()
}


def map_tags_to_genres(tags, limit=None):
    """Chỉ gán thể loại khi có cụm từ trong tag/category làm bằng chứng."""
    found = set()
    for tag in tags:
        normalized = normalize_tag(tag)
        padded = f" {normalized} "
        for genre in GENRES:
            if any(f" {alias} " in padded for alias in _NORMALIZED_ALIASES[genre]):
                found.add(genre)
    ordered = [genre for genre in GENRES if genre in found]
    return ordered[:limit] if limit is not None else ordered


def matches_genres(book_genres, selected_genres):
    """Lọc OR: giữ sách khớp ít nhất một thể loại được chọn."""
    return not selected_genres or bool(set(book_genres or []) & set(selected_genres))
