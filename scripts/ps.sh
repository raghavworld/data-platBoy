#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
compose_base \
  -f "$CORE_FILE" \
  -f "$STORAGE_FILE" \
  -f "$QUERY_FILE" \
  -f "$COMPUTE_FILE" \
  -f "$BI_FILE" \
  -f "$GOVERNANCE_FILE" \
  ps -a
