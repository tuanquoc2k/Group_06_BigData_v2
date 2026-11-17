"""Create required MongoDB indexes without dropping or rewriting existing data."""
from common.mongo_indexes import ensure_indexes
from common.settings import get_db


if __name__ == "__main__":
    ensure_indexes(get_db())
    print("BookPulse MongoDB indexes are ready.")
