#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
print_group "Stopping STORAGE group"
stop_and_remove "$STORAGE_FILE" "${STORAGE_SERVICES[@]}"
print_ps
