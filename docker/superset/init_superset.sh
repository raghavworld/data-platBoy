#!/usr/bin/env bash
set -euo pipefail

superset db upgrade
superset fab create-admin \
  --username "${SUPERSET_USER:-admin}" \
  --firstname Local \
  --lastname Admin \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
  --password "${SUPERSET_PASSWORD:-admin}" || true
superset init
