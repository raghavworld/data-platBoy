from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Any

import requests

from common import setup_logging


def api_base() -> str:
    if Path("/.dockerenv").exists():
        return os.environ.get("DASHBOARD_PUBLIC_API_BASE_URL", "http://localhost:8001")
    return os.environ.get("DASHBOARD_PUBLIC_API_BASE_URL", f"http://localhost:{os.environ.get('DASHBOARD_API_PORT', '8001')}")


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def request_json(method: str, path: str, token: str | None = None, expect_ok: bool = True, **kwargs: Any) -> tuple[int, Any]:
    headers = kwargs.pop("headers", {})
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = requests.request(method, f"{api_base()}{path}", headers=headers, timeout=kwargs.pop("timeout", 180), **kwargs)
    if expect_ok:
        response.raise_for_status()
    try:
        payload = response.json()
    except Exception:
        payload = response.text
    return response.status_code, payload


def login() -> str:
    _, payload = request_json(
        "POST",
        "/api/auth/login",
        json={"username": os.environ["ONOV8_ADMIN_USERNAME"], "password": os.environ["ONOV8_ADMIN_PASSWORD"]},
        timeout=30,
    )
    token = payload.get("token")
    assert_ok(bool(token), "Admin login did not return a token")
    return token


def validate_health_after_restart(token: str) -> dict[str, Any]:
    _, health = request_json("GET", "/api/platform/full-health", token, timeout=180)
    assert_ok(health.get("status") in {"ok", "degraded"}, f"Full health did not load after restart flow: {health}")
    assert_ok("runtime" in health and "services" in health, "Full health is missing runtime or services")
    return health


def validate_restart_safe_behavior(token: str, live_restart: bool) -> None:
    _, dry_restart = request_json(
        "POST",
        "/api/operations/services/trino/restart",
        token,
        json={"dry_run": True},
        timeout=120,
    )
    assert_ok(dry_restart.get("status") == "dry_run", f"Dry-run restart failed: {dry_restart}")

    _, restart_all = request_json(
        "POST",
        "/api/operations/global/restart-all",
        token,
        json={"dry_run": True, "service_names": ["trino", "superset", "dashboard-web"]},
        timeout=180,
    )
    assert_ok(restart_all.get("status") in {"ok", "dry_run"}, f"Restart-all dry-run failed: {restart_all}")

    if live_restart:
        _, live = request_json(
            "POST",
            "/api/operations/services/dashboard-web/restart",
            token,
            json={"dry_run": False},
            timeout=240,
        )
        assert_ok(live.get("status") in {"ok", "error"}, f"Live restart returned unexpected payload: {live}")
        time.sleep(5)
        validate_health_after_restart(token)


def validate_startup_race_signals(token: str) -> None:
    snapshots = []
    for _ in range(3):
        snapshots.append(validate_health_after_restart(token))
        time.sleep(2)
    service_counts = [len(snapshot.get("services", [])) for snapshot in snapshots]
    assert_ok(min(service_counts) >= 8, f"Health aggregation is missing services during repeated checks: {service_counts}")
    for snapshot in snapshots:
        containers = snapshot.get("runtime", {}).get("containers", [])
        assert_ok(isinstance(containers, list), "Runtime container snapshot is not a list")


def validate_backup_survives_restart_flow(token: str) -> None:
    _, backup = request_json(
        "POST",
        "/api/platform/backup/export",
        token,
        json={"backup_type": "pipeline_state"},
        timeout=240,
    )
    backup_row = backup.get("backup") or backup.get("result", {}).get("backup")
    assert_ok(backup_row and backup_row.get("status") == "ok", f"Pipeline state backup failed: {backup}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live-restart", action="store_true", help="Actually restart dashboard-web after dry-run checks")
    args = parser.parse_args()

    logger = setup_logging("validate_platform_resilience")
    logger.info("Validating Phase 8 platform resilience")

    token = login()
    validate_restart_safe_behavior(token, live_restart=args.live_restart or os.environ.get("LIVE_RESILIENCE_RESTART", "false").lower() == "true")
    validate_startup_race_signals(token)
    validate_backup_survives_restart_flow(token)

    logger.info("Phase 8 resilience validation passed")


if __name__ == "__main__":
    main()
