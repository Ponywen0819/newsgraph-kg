#!/usr/bin/env bash
# 用法：
#   ./run.sh ingest articles.json     |  cat x.json | ./run.sh ingest -
#   ./run.sh query search "..."       |  ./run.sh query stats
#   ./run.sh -m newsgraph.<模組> [args...]   # 直接跑套件內任一模組
set -euo pipefail
cd "$(dirname "$0")"
case "${1:-}" in
  ingest)   shift; set -- -m newsgraph.ingest   "$@" ;;
  query)    shift; set -- -m newsgraph.query    "$@" ;;
  reporter) shift; set -- -m newsgraph.reporter "$@" ;;
esac
# -i：把 stdin 傳進容器（ingest - 從 stdin 讀新聞時必要）
podman run --rm -i --network host \
  --env-file .env \
  -v "$PWD":/app:ro,z \
  -v newsgraph-state:/app/state \
  localhost/newsgraph:latest "$@"
