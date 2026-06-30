#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
print_group "Stopping CORE group"
stop_and_remove "$CORE_FILE" "${CORE_SERVICES[@]}"
print_ps
