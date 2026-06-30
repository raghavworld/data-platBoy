#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="local-data-platform-modular"
NETWORK_NAME="onov8-data-platform-net"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${ONOV8_PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
SCRIPTS_DIR="${ONOV8_RUNTIME_SCRIPTS_DIR:-$SCRIPT_DIR}"
ENV_FILE="$ROOT_DIR/.env"

CORE_FILE="$ROOT_DIR/compose/docker-compose.core.yml"
STORAGE_FILE="$ROOT_DIR/compose/docker-compose.storage.yml"
QUERY_FILE="$ROOT_DIR/compose/docker-compose.query.yml"
COMPUTE_FILE="$ROOT_DIR/compose/docker-compose.compute.yml"
BI_FILE="$ROOT_DIR/compose/docker-compose.bi.yml"
GOVERNANCE_FILE="$ROOT_DIR/compose/docker-compose.governance.yml"

CORE_SERVICES=(
  dashboard-postgres
  airflow-postgres
  hive-metastore
  airflow-init
  airflow-webserver
  airflow-scheduler
  dashboard-api
  dashboard-web
)

STORAGE_SERVICES=(minio minio-init)
QUERY_SERVICES=(trino)
COMPUTE_SERVICES=(spark)
BI_SERVICES=(superset-init superset)
GOVERNANCE_SERVICES=(metadata-db metadata-search openmetadata)

ensure_env() {
  if [ ! -f "$ENV_FILE" ]; then
    echo "Missing .env. Create one first: cp .env.example .env"
    exit 1
  fi
}

ensure_network() {
  if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
    echo "Creating shared Docker network: $NETWORK_NAME"
    docker network create "$NETWORK_NAME" >/dev/null
  fi
}

compose_base() {
  docker compose --env-file "$ENV_FILE" -p "$PROJECT_NAME" "$@"
}

print_group() {
  echo "==> $1"
}

print_ps() {
  "$SCRIPTS_DIR/ps.sh"
}

stop_and_remove() {
  local compose_file="$1"
  shift
  compose_base -f "$compose_file" stop "$@" || true
  compose_base -f "$compose_file" rm -f "$@" || true
}

is_running() {
  local container="$1"
  [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)" = "true" ]
}

require_running() {
  local container="$1"
  local message="$2"
  if ! is_running "$container"; then
    echo "$message"
    exit 1
  fi
}
