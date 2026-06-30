#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env
print_group "Stopping HEAVY groups: QUERY, COMPUTE, BI, GOVERNANCE"
compose_base -f "$QUERY_FILE" -f "$COMPUTE_FILE" -f "$BI_FILE" -f "$GOVERNANCE_FILE" stop "${QUERY_SERVICES[@]}" "${COMPUTE_SERVICES[@]}" "${BI_SERVICES[@]}" "${GOVERNANCE_SERVICES[@]}" || true
compose_base -f "$QUERY_FILE" -f "$COMPUTE_FILE" -f "$BI_FILE" -f "$GOVERNANCE_FILE" rm -f "${QUERY_SERVICES[@]}" "${COMPUTE_SERVICES[@]}" "${BI_SERVICES[@]}" "${GOVERNANCE_SERVICES[@]}" || true
print_ps
