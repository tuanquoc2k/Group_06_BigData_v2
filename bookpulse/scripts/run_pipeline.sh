#!/usr/bin/env bash
# Run the whole BookPulse batch pipeline on Goodbooks-10k with one command, in order, from bookpulse/:
#   download -> clean -> Spark SQL -> ALS explicit + implicit -> write recommendations
#   -> read MongoDB back (NoSQL evidence) -> offline fold-in evaluation
# Every step writes measurements to reports/; total elapsed time is recorded in reports/pipeline_*.json.
#
#   docker compose up -d
#   bash scripts/run_pipeline.sh
# The MapReduce component runs on its own Hadoop cluster: see README section 8.
set -euo pipefail
cd "$(dirname "$0")/.."
pipeline_started=$(date +%s)

run() {
  echo
  echo "=== $(date +%H:%M:%S) $*"
  docker compose exec -T spark "$@"
}

run python scripts/download_goodbooks.py
run python batch/clean.py
run python batch/analytics_sql.py --books data/goodbooks/books.csv --raw-csv data/goodbooks/ratings.csv
run python batch/train_als.py
run python batch/train_als.py --implicit
run python batch/write_recs.py --books data/goodbooks/books.csv
run python scripts/nosql_evidence.py
run python scripts/foldin_eval.py --eval-users 1000
pipeline_seconds=$(( $(date +%s) - pipeline_started ))
docker compose exec -T -e BOOKPULSE_PIPELINE_SECONDS="$pipeline_seconds" spark python -c \
  'import os; from common.settings import save_report; save_report("pipeline", {"total_seconds": int(os.environ["BOOKPULSE_PIPELINE_SECONDS"]), "includes_write_recs": True})'
echo
echo "=== $(date +%H:%M:%S) done in ${pipeline_seconds}s. Results: reports/ ; app: http://localhost:8501"
