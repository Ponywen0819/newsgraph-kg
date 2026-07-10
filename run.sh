#!/usr/bin/env bash
# 用法：
#   ./run.sh ingest articles.json     |  cat x.json | ./run.sh ingest -
#   ./run.sh query search "..."       |  ./run.sh query stats
#   ./run.sh <任意.py> [args...]
set -euo pipefail
cd "$(dirname "$0")"
case "${1:-}" in
  ingest)   shift; set -- ingest.py   "$@" ;;
  query)    shift; set -- query.py    "$@" ;;
  reporter) shift; set -- reporter.py "$@" ;;
esac
# -i：把 stdin 傳進容器（ingest - 從 stdin 讀新聞時必要）
podman run --rm -i --network host \
  --env-file .env \
  -v "$PWD":/app:ro,z \
  -v newsgraph-state:/app/state \
  localhost/newsgraph:latest "$@"
