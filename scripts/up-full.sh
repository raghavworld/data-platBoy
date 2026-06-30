#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
ensure_network
print_group "Starting FULL stack"
compose_base \
  -f "$CORE_FILE" \
  -f "$STORAGE_FILE" \
  -f "$QUERY_FILE" \
  -f "$COMPUTE_FILE" \
  -f "$BI_FILE" \
  -f "$GOVERNANCE_FILE" \
  up -d --build
compose_base -f "$CORE_FILE" rm -f airflow-init >/dev/null 2>&1 || true
compose_base -f "$STORAGE_FILE" rm -f minio-init >/dev/null 2>&1 || true
compose_base -f "$BI_FILE" rm -f superset-init >/dev/null 2>&1 || true
print_ps
