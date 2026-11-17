"""Paths, connection settings and small helpers shared by every BookPulse job."""
import json
import os
import platform
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(os.getenv("BOOKPULSE_HOME", Path(__file__).resolve().parents[1]))
DATA_DIR = ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
STREAM_EVENTS_DIR = PARQUET_DIR / "stream_events"  # valid streamed ratings, merged in by batch/clean.py
MODEL_DIR = ROOT / "models"
REPORT_DIR = ROOT / "reports"
CHECKPOINT_DIR = ROOT / "checkpoints"
ALS_MODEL_NAME = os.getenv("ALS_MODEL_NAME", "als_implicit")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "bookpulse")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rating-events")
ALS_MIN_RATING = int(os.getenv("ALS_MIN_RATING", "4"))
ALS_RANKS = os.getenv("ALS_RANKS", "20,50")
ALS_REGS = os.getenv("ALS_REGS", "0.1")
ALS_ALPHAS = os.getenv("ALS_ALPHAS", "1,10")
ALS_MAX_ITER = int(os.getenv("ALS_MAX_ITER", "10"))
ALS_TEST_RATIO = float(os.getenv("ALS_TEST_RATIO", "0.2"))
ALS_EVAL_USERS = int(os.getenv("ALS_EVAL_USERS", "5000"))
ALS_SEED = int(os.getenv("ALS_SEED", "42"))
FOLD_IN_MIN_RATINGS = int(os.getenv("FOLD_IN_MIN_RATINGS", "3"))
FOLD_IN_TOP_K = int(os.getenv("FOLD_IN_TOP_K", "100"))
RECOMMENDATION_TOP_K = int(os.getenv("RECOMMENDATION_TOP_K", "100"))
FOLD_IN_POLL_SECONDS = float(os.getenv("FOLD_IN_POLL_SECONDS", "1"))
FOLD_IN_DEBOUNCE_SECONDS = float(os.getenv("FOLD_IN_DEBOUNCE_SECONDS", "0.5"))
FOLD_IN_JOB_TIMEOUT_SECONDS = int(os.getenv("FOLD_IN_JOB_TIMEOUT_SECONDS", "120"))
FOLD_IN_MAX_RETRIES = int(os.getenv("FOLD_IN_MAX_RETRIES", "3"))
STREAM_TRIGGER = os.getenv("STREAM_TRIGGER", "5 seconds")
STREAM_WATERMARK = os.getenv("STREAM_WATERMARK", "10 minutes")
STREAM_MAX_OFFSETS = int(os.getenv("STREAM_MAX_OFFSETS", "20000"))
STREAM_PARTITIONS = int(os.getenv("STREAM_PARTITIONS", "3"))
STREAM_RESTART_SECONDS = float(os.getenv("STREAM_RESTART_SECONDS", "5"))
KAFKA_LOG_RETENTION_MS = int(os.getenv("KAFKA_LOG_RETENTION_MS", str(7 * 24 * 60 * 60 * 1000)))
MONGO_SERVER_SELECTION_TIMEOUT_MS = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000"))

if not 1 <= ALS_MIN_RATING <= 5:
    raise ValueError("ALS_MIN_RATING must be between 1 and 5")
if FOLD_IN_MIN_RATINGS < 1:
    raise ValueError("FOLD_IN_MIN_RATINGS must be at least 1")
if FOLD_IN_TOP_K < 1 or RECOMMENDATION_TOP_K < 1:
    raise ValueError("Recommendation top-k values must be at least 1")
if FOLD_IN_POLL_SECONDS <= 0 or FOLD_IN_DEBOUNCE_SECONDS < 0:
    raise ValueError("Worker polling must be positive and debounce cannot be negative")
if FOLD_IN_JOB_TIMEOUT_SECONDS < 1 or FOLD_IN_MAX_RETRIES < 1:
    raise ValueError("Worker timeout and retry count must be at least 1")
if STREAM_MAX_OFFSETS < 1 or STREAM_PARTITIONS < 1 or STREAM_RESTART_SECONDS <= 0:
    raise ValueError("Streaming offsets, partitions and restart delay must be positive")
if KAFKA_LOG_RETENTION_MS < 1:
    raise ValueError("Kafka log retention must be positive")
if MONGO_SERVER_SELECTION_TIMEOUT_MS < 1:
    raise ValueError("MONGO_SERVER_SELECTION_TIMEOUT_MS must be positive")


def current_model(name):
    """Return the versioned model path and manifest, with support for pre-versioned models."""
    model_root = MODEL_DIR / name
    pointer_path = model_root / "current.json"
    if not pointer_path.exists():
        return model_root, {}
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    relative_path = Path(pointer["path"])
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ValueError(f"Invalid model path in {pointer_path}: {relative_path}")
    model_path = model_root / relative_path
    manifest_path = model_path / "manifest.json"
    if not model_path.is_dir() or not manifest_path.is_file():
        raise FileNotFoundError(f"Incomplete model version referenced by {pointer_path}")
    return model_path, json.loads(manifest_path.read_text(encoding="utf-8"))


def get_spark(app_name, kafka=False):
    """Local Spark session; SPARK_MASTER=local[1|2|4] is how the core-scaling runs are made."""
    import pyspark
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    if kafka:
        builder = builder.config(
            "spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}",
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


_client = None


def get_db():
    """One MongoClient per process (it is thread-safe and pools connections), reused by every batch."""
    global _client
    from pymongo import MongoClient

    if _client is None:
        _client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=MONGO_SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=MONGO_SERVER_SELECTION_TIMEOUT_MS,
            retryWrites=True,
        )
    return _client[MONGO_DB]


def save_report(name, payload):
    """Write a measurement and reproducibility context to reports/<name>_<timestamp>.json."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    payload.setdefault("run_metadata", {
        "commit_hash": os.getenv("BOOKPULSE_COMMIT_HASH") or None,
        "working_tree_dirty": os.getenv("BOOKPULSE_WORKTREE_DIRTY"),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "configuration": {
            "als_model_name": ALS_MODEL_NAME,
            "als_min_rating": ALS_MIN_RATING,
            "fold_in_min_ratings": FOLD_IN_MIN_RATINGS,
            "fold_in_top_k": FOLD_IN_TOP_K,
            "fold_in_job_timeout_seconds": FOLD_IN_JOB_TIMEOUT_SECONDS,
            "fold_in_max_retries": FOLD_IN_MAX_RETRIES,
            "stream_trigger": STREAM_TRIGGER,
            "stream_watermark": STREAM_WATERMARK,
            "stream_max_offsets": STREAM_MAX_OFFSETS,
            "kafka_log_retention_ms": KAFKA_LOG_RETENTION_MS,
        },
    })
    path = REPORT_DIR / f"{name}_{time.strftime('%Y%m%d_%H%M%S')}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    print(f"-> {path.relative_to(ROOT)}")
    return path
