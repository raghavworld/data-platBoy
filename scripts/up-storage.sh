#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
ensure_network
print_group "Starting STORAGE group"
compose_base -f "$STORAGE_FILE" up -d
compose_base -f "$STORAGE_FILE" rm -f minio-init >/dev/null 2>&1 || true
print_ps
