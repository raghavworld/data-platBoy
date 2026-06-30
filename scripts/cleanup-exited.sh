#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/_common.sh"
ensure_env

mapfile -t exited_containers < <(
  docker ps -a \
    --filter "label=com.docker.compose.project=$PROJECT_NAME" \
    --filter "status=exited" \
    --format "{{.Names}}"
)

if [ "${#exited_containers[@]}" -eq 0 ]; then
  echo "No exited containers found for project: $PROJECT_NAME"
  exit 0
fi

printf 'Removing exited containers for project %s:\n' "$PROJECT_NAME"
printf '  %s\n' "${exited_containers[@]}"
docker rm "${exited_containers[@]}"

cat <<'MSG'
Exited project containers removed. Running containers, images, volumes, and bind-mounted data were not touched.
Useful next command:
  ./scripts/ps.sh
MSG
