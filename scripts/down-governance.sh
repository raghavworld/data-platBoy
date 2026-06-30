#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
print_group "Stopping GOVERNANCE group"
stop_and_remove "$GOVERNANCE_FILE" "${GOVERNANCE_SERVICES[@]}"
print_ps
