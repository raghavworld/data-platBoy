#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
print_group "Stopping QUERY group"
stop_and_remove "$QUERY_FILE" "${QUERY_SERVICES[@]}"
print_ps
