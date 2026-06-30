#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
ensure_network
print_group "Restarting CORE group"
"$SCRIPTS_DIR/down-core.sh"
"$SCRIPTS_DIR/up-core.sh"
