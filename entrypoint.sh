#!/usr/bin/env sh
# Process entrypoint: role = webapp | analyzer-a | analyzer-b
set -e

ROLE="${1:-webapp}"

case "$ROLE" in
  webapp)
    exec uvicorn app.webapp.main:app --host 0.0.0.0 --port 8000
    ;;
  analyzer-a)
    export ANALYZER_ID="analyzer-a"
    export ANALYZER_DB="${ANALYZER_DB:-/data/analyzer-a.db}"
    exec uvicorn app.analyzer.app:app --host 0.0.0.0 --port 8001
    ;;
  analyzer-b)
    export ANALYZER_ID="analyzer-b"
    export ANALYZER_DB="${ANALYZER_DB:-/data/analyzer-b.db}"
    exec uvicorn app.analyzer.app:app --host 0.0.0.0 --port 8002
    ;;
  *)
    echo "unknown role: $ROLE (expected webapp|analyzer-a|analyzer-b)" >&2
    exit 2
    ;;
esac
