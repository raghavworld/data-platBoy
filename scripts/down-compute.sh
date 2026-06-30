#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
print_group "Stopping COMPUTE group"
stop_and_remove "$COMPUTE_FILE" "${COMPUTE_SERVICES[@]}"
print_ps
