#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
ensure_network
require_running local-data-platform-modular-trino "BI needs Trino first. Run: ./scripts/up-query.sh"
require_running local-data-platform-modular-airflow-postgres "BI needs CORE database first. Run: ./scripts/up-core.sh"
print_group "Starting BI group"
compose_base -f "$BI_FILE" up -d --build
compose_base -f "$BI_FILE" rm -f superset-init >/dev/null 2>&1 || true
print_ps
