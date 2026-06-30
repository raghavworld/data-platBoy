#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
ensure_network
require_running local-data-platform-modular-minio "COMPUTE needs STORAGE first. Run: ./scripts/up-storage.sh"
require_running local-data-platform-modular-hive-metastore "COMPUTE needs CORE hive-metastore first. Run: ./scripts/up-core.sh"
print_group "Starting COMPUTE group"
compose_base -f "$COMPUTE_FILE" up -d
print_ps
