#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
ensure_network
print_group "Starting CORE group"
compose_base -f "$CORE_FILE" up -d --build
compose_base -f "$CORE_FILE" rm -f airflow-init >/dev/null 2>&1 || true
print_ps
