#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
ensure_network
require_running local-data-platform-modular-minio "QUERY needs STORAGE first. Run: ./scripts/up-storage.sh"
require_running local-data-platform-modular-hive-metastore "QUERY needs CORE hive-metastore first. Run: ./scripts/up-core.sh"
print_group "Starting QUERY group"
compose_base -f "$QUERY_FILE" up -d
print_ps
