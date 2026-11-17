"""Fold-in suy luận cho implicit ALS, giữ nguyên toàn bộ vector sách."""
from dataclasses import dataclass
import json
import logging
from pathlib import Path

import numpy as np

from common.settings import ALS_MIN_RATING, FOLD_IN_MIN_RATINGS, FOLD_IN_TOP_K

log = logging.getLogger("bookpulse-foldin")


@dataclass(frozen=True)
class FoldInModel:
    """Artifact ALS đã nạp cùng chỉ mục sách để tra cứu nhanh."""

    item_ids: np.ndarray
    factors: np.ndarray
    gram: np.ndarray
    item_index: dict
    version: str
    alpha: float
    reg_param: float
    rank: int
    min_rating: int
    min_ratings: int = FOLD_IN_MIN_RATINGS
    top_k: int = FOLD_IN_TOP_K


def load_model(path, active_model_version=None):
    """Nạp artifact và kiểm tra metadata, shape, kiểu mô hình và phiên bản."""
    path = Path(path)
    manifest_path = path / "manifest.json"
    npy_paths = (path / "item_ids.npy", path / "item_factors.npy", path / "YtY.npy")
    npz_path = path / "item_factors.npz"
    if not manifest_path.is_file() or not (
        all(file.is_file() for file in npy_paths) or npz_path.is_file()
    ):
        raise FileNotFoundError(f"Thiếu manifest hoặc item factors trong {path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    implicit = manifest.get("implicitPrefs", manifest.get("implicit"))
    min_rating = manifest.get("rating_threshold", manifest.get("min_rating"))
    required = ("model_version", "rank", "alpha", "regParam")
    missing = [key for key in required if manifest.get(key) is None]
    if min_rating is None:
        missing.append("rating_threshold/min_rating")
    if missing:
        raise ValueError(f"Thiếu metadata fold-in: {', '.join(missing)}")
    if implicit is not True:
        raise ValueError("Chỉ hỗ trợ artifact Spark ALS với implicitPrefs=True")
    if int(min_rating) != ALS_MIN_RATING:
        raise ValueError(
            f"rating threshold của model ({min_rating}) khác ALS_MIN_RATING ({ALS_MIN_RATING})"
        )

    if all(file.is_file() for file in npy_paths):
        item_ids = np.load(npy_paths[0], allow_pickle=False).astype(np.int64, copy=False)
        factors = np.load(npy_paths[1], allow_pickle=False).astype(np.float64, copy=False)
        gram = np.load(npy_paths[2], allow_pickle=False).astype(np.float64, copy=False)
    else:
        with np.load(npz_path, allow_pickle=False) as artifact:
            if not {"item_ids", "factors", "YtY"}.issubset(artifact.files):
                raise ValueError(f"{npz_path} cần chứa item_ids, factors và YtY")
            item_ids = artifact["item_ids"].astype(np.int64, copy=True)
            factors = artifact["factors"].astype(np.float64, copy=True)
            gram = artifact["YtY"].astype(np.float64, copy=True)

    rank = int(manifest["rank"])
    if (item_ids.ndim != 1 or len(item_ids) == 0 or factors.ndim != 2
            or factors.shape != (len(item_ids), rank) or gram.shape != (rank, rank)):
        raise ValueError("Shape item_ids/factors/YtY không khớp rank trong metadata")
    if len(set(map(int, item_ids))) != len(item_ids):
        raise ValueError("item_ids chứa mã sách trùng lặp")
    if not (np.isfinite(factors).all() and np.isfinite(gram).all()):
        raise ValueError("Artifact item factors chứa giá trị không hữu hạn")
    if not np.allclose(gram, factors.T @ factors, rtol=1e-8, atol=1e-8):
        raise ValueError("YtY không khớp với toàn bộ item factors trong artifact")

    version = str(manifest["model_version"])
    if active_model_version and version != str(active_model_version):
        log.warning("Artifact model_version=%s khác active MongoDB=%s",
                    version, active_model_version)
    model = FoldInModel(
        item_ids=item_ids,
        factors=factors,
        gram=gram,
        item_index={int(book_id): index for index, book_id in enumerate(item_ids)},
        version=version,
        alpha=float(manifest["alpha"]),
        reg_param=float(manifest["regParam"]),
        rank=rank,
        min_rating=int(min_rating),
    )
    if not np.isfinite(model.alpha) or not np.isfinite(model.reg_param):
        raise ValueError("alpha/regParam phải là số hữu hạn")
    return model


def fold_in_user(item_ids, item_factors, ratings, alpha, reg_param, gram=None,
                 rating_threshold=ALS_MIN_RATING, item_index=None):
    """Giải normal equation bằng float64; YtY phải gồm toàn bộ item factors."""
    if item_index is None:
        item_index = {int(book_id): index for index, book_id in enumerate(item_ids)}
    observed = []
    unknown_count = 0
    for book_id, rating in ratings:
        book_id, rating = int(book_id), float(rating)
        if rating < rating_threshold:
            continue
        index = item_index.get(book_id)
        if index is None:
            unknown_count += 1
            continue
        observed.append((item_factors[index], rating))

    if unknown_count:
        log.warning("Bỏ qua %d rating dương có book_id không nằm trong itemFactors", unknown_count)
    if not observed:
        log.warning("Không có rating dương hợp lệ để fold-in")
        return None

    factors64 = np.asarray(item_factors, dtype=np.float64)
    base_gram = factors64.T @ factors64 if gram is None else np.asarray(gram, dtype=np.float64)
    y = np.asarray([factor for factor, _ in observed], dtype=np.float64)
    c1 = np.asarray([alpha * rating for _, rating in observed], dtype=np.float64)
    lhs = base_gram + (y.T * c1) @ y
    lhs.flat[::lhs.shape[0] + 1] += reg_param * len(observed)
    rhs = y.T @ (1.0 + c1)
    try:
        return np.linalg.solve(lhs, rhs)
    except np.linalg.LinAlgError:
        log.exception("Không giải được hệ fold-in: ma trận suy biến hoặc không hợp lệ")
        raise


def fold_in(ratings, model):
    """Tính vector user; trả None nếu ít hơn ngưỡng rating tích cực đã cấu hình."""
    ratings = list(ratings)
    threshold = getattr(model, "min_rating", ALS_MIN_RATING)
    item_index = getattr(
        model, "item_index",
        {int(book_id): index for index, book_id in enumerate(model.item_ids)},
    )
    known_positive = [
        (int(book_id), float(rating))
        for book_id, rating in ratings
        if float(rating) >= threshold and int(book_id) in item_index
    ]
    unknown_count = sum(
        1 for book_id, rating in ratings
        if float(rating) >= threshold and int(book_id) not in item_index
    )
    if unknown_count:
        log.warning("Bỏ qua %d sách mới/không có trong model", unknown_count)
    min_ratings = getattr(model, "min_ratings", FOLD_IN_MIN_RATINGS)
    if not known_positive:
        log.warning("User không có rating dương thuộc itemFactors")
        return None
    if len(known_positive) < min_ratings:
        log.info("Fold-in chưa đủ rating dương: có %d, cần %d",
                 len(known_positive), min_ratings)
        return None
    return fold_in_user(
        model.item_ids, model.factors, known_positive, model.alpha, model.reg_param,
        model.gram, threshold, item_index,
    )


def recommend(user_ratings, seen_ids, model, k=None):
    """Fold-in rồi chấm toàn bộ sách bằng Y @ x, loại sách đã chấm."""
    user_ratings = list(user_ratings)
    seen = {int(book_id) for book_id in seen_ids}
    seen.update(int(book_id) for book_id, _ in user_ratings)
    vector = fold_in(user_ratings, model)
    if vector is None:
        return []
    return recommend_from_vector(vector, seen, model, k)


def recommend_from_vector(vector, seen_ids, model, k=None):
    """Xếp hạng từ vector đã tính để worker dùng chung phép fold-in cho log và ranking."""
    k = model.top_k if k is None and hasattr(model, "top_k") else (
        FOLD_IN_TOP_K if k is None else int(k)
    )
    if k <= 0:
        raise ValueError("k phải lớn hơn 0")
    seen = {int(book_id) for book_id in seen_ids}
    vector = np.asarray(vector, dtype=np.float64)
    if vector.shape != (model.rank,) or not np.isfinite(vector).all():
        raise ValueError("Vector user không hợp lệ hoặc không khớp rank của model")

    candidate_indexes = np.asarray(
        [index for index, book_id in enumerate(model.item_ids) if int(book_id) not in seen],
        dtype=np.int64,
    )
    if candidate_indexes.size == 0:
        log.warning("Không còn sách chưa chấm để gợi ý")
        return []
    scores = model.factors @ vector
    candidate_scores = scores[candidate_indexes]
    count = min(k, len(candidate_indexes))
    if count < len(candidate_indexes):
        selected = np.argpartition(candidate_scores, -count)[-count:]
        candidate_indexes = candidate_indexes[selected]
    order = np.lexsort((model.item_ids[candidate_indexes], -scores[candidate_indexes]))
    return [
        (int(model.item_ids[index]), float(scores[index]))
        for index in candidate_indexes[order]
    ]
