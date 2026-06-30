#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
print_group "Stopping BI group"
stop_and_remove "$BI_FILE" "${BI_SERVICES[@]}"
print_ps
