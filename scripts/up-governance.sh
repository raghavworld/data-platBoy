#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
ensure_network
print_group "Starting GOVERNANCE group"
compose_base -f "$GOVERNANCE_FILE" up -d
print_ps
