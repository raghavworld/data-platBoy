from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

from common import PROJECT_ROOT, setup_logging


EXPECTED_HEALTHCHECK_SERVICES = {
    "airflow-postgres",
    "dashboard-postgres",
    "mongo-users",
    "mongo-orders",
    "mongo-products",
    "mongo-payments",
    "minio",
    "hive-metastore",
    "spark",
    "trino",
    "airflow-webserver",
    "airflow-scheduler",
    "superset",
    "dashboard-api",
    "dashboard-web",
}

PREVIOUS_PHASE_COMMANDS = [
    ["python", "/opt/platform/scripts/validate_raw_phase.py"],
    ["python", "/opt/platform/scripts/validate_bronze_phase.py"],
    ["python", "/opt/platform/scripts/validate_silver_phase.py"],
    ["python", "/opt/platform/scripts/validate_query_phase.py", "--skip-make-check"],
    ["python", "/opt/platform/scripts/validate_bi_phase.py", "--skip-make-check"],
    ["python", "/opt/platform/scripts/validate_governance_phase.py", "--skip-sync"],
    ["python", "/opt/platform/scripts/validate_operations_center.py", "--skip-previous-phase-scripts"],
]


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


def login(username_env: str, password_env: str) -> str:
    username = os.environ[username_env]
    password = os.environ[password_env]
    _, payload = request_json("POST", "/api/auth/login", json={"username": username, "password": password}, timeout=30)
    token = payload.get("token")
    assert_ok(bool(token), f"Login did not return a token for {username_env}")
    return token


def validate_auth_and_rbac() -> dict[str, Any]:
    unauth_status, _ = request_json("GET", "/api/sources", expect_ok=False, timeout=30)
    assert_ok(unauth_status == 401, f"Protected route did not require auth: HTTP {unauth_status}")

    admin_token = login("ONOV8_ADMIN_USERNAME", "ONOV8_ADMIN_PASSWORD")
    _, me = request_json("GET", "/api/auth/me", admin_token, timeout=30)
    assert_ok(me.get("user", {}).get("role") == "admin", f"Admin role mismatch: {me}")

    viewer_token = login("ONOV8_VIEWER_USERNAME", "ONOV8_VIEWER_PASSWORD")
    _, sources = request_json("GET", "/api/sources", viewer_token, timeout=30)
    assert_ok(isinstance(sources, list), "Viewer could not read sources")
    denied_status, _ = request_json(
        "POST",
        "/api/operations/raw/flush",
        viewer_token,
        expect_ok=False,
        json={"confirmation": "FLUSH RAW"},
        timeout=30,
    )
    assert_ok(denied_status == 403, f"Viewer destructive operation was not forbidden: HTTP {denied_status}")

    operator_token = login("ONOV8_OPERATOR_USERNAME", "ONOV8_OPERATOR_PASSWORD")
    _, validation = request_json("POST", "/api/operations/global/validate", operator_token, timeout=240)
    assert_ok(validation.get("status") in {"ok", "failed"}, f"Operator validation did not run: {validation}")
    operator_denied, _ = request_json(
        "POST",
        "/api/operations/services/trino/restart",
        operator_token,
        expect_ok=False,
        json={"dry_run": True},
        timeout=30,
    )
    assert_ok(operator_denied == 403, f"Operator restart was not forbidden: HTTP {operator_denied}")
    return {"admin_token": admin_token, "operator_token": operator_token, "viewer_token": viewer_token}


def validate_observability(admin_token: str) -> None:
    _, health = request_json("GET", "/api/platform/full-health", admin_token, timeout=180)
    assert_ok(health.get("status") in {"ok", "degraded"}, f"Full health failed: {health}")
    assert_ok("runtime" in health and "resources" in health and "pipelines" in health, "Full health is missing runtime/resource/pipeline sections")

    _, alert_sync = request_json("POST", "/api/platform/alerts/sync", admin_token, timeout=120)
    assert_ok(alert_sync.get("status") == "ok", f"Alert sync failed: {alert_sync}")
    _, alerts = request_json("GET", "/api/platform/alerts", admin_token, timeout=120)
    assert_ok(isinstance(alerts, list), "Alerts endpoint did not return a list")
    assert_ok(alerts, "Alerts endpoint did not contain a created alert")

    _, audit = request_json("GET", "/api/platform/audit-logs", admin_token, timeout=60)
    assert_ok(any(item.get("actor") for item in audit), "Audit logs did not capture any actor")

    _, sessions = request_json("GET", "/api/platform/sessions", admin_token, timeout=60)
    assert_ok(sessions, "User sessions endpoint returned no sessions")

    _, security = request_json("GET", "/api/platform/security-overview", admin_token, timeout=60)
    assert_ok(security.get("auth_enabled") is True, "Security overview does not report auth enabled")


def validate_backup(admin_token: str) -> None:
    _, backup = request_json(
        "POST",
        "/api/platform/backup/export",
        admin_token,
        json={"backup_type": "metadata_snapshot"},
        timeout=300,
    )
    backup_row = backup.get("backup") or (backup.get("result") or {}).get("backup")
    assert_ok(backup_row and backup_row.get("status") == "ok", f"Backup export failed: {backup}")
    backup_path = Path(backup_row["file_path"])
    assert_ok(backup_path.exists() and backup_path.stat().st_size > 0, f"Backup file was not created: {backup_path}")

    _, restore = request_json(
        "POST",
        "/api/platform/backup/restore",
        admin_token,
        json={"file_name": backup_path.name, "dry_run": True},
        timeout=120,
    )
    assert_ok(restore.get("status") == "dry_run" or restore.get("result", {}).get("status") == "dry_run", f"Restore dry-run failed: {restore}")


def service_blocks(compose_text: str) -> dict[str, str]:
    blocks: dict[str, str] = {}
    current_name = None
    current_lines: list[str] = []
    for line in compose_text.splitlines():
        match = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if match:
            if current_name:
                blocks[current_name] = "\n".join(current_lines)
            current_name = match.group(1)
            current_lines = []
            continue
        if current_name:
            current_lines.append(line)
    if current_name:
        blocks[current_name] = "\n".join(current_lines)
    return blocks


def validate_compose_hardening() -> None:
    compose = (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    blocks = service_blocks(compose)
    missing_healthchecks = sorted(service for service in EXPECTED_HEALTHCHECK_SERVICES if "healthcheck:" not in blocks.get(service, ""))
    assert_ok(not missing_healthchecks, f"Missing healthchecks: {missing_healthchecks}")
    missing_restarts = sorted(
        service
        for service, block in blocks.items()
        if service not in {"minio-init", "airflow-init", "superset-init"} and "restart: unless-stopped" not in block
    )
    assert_ok(not missing_restarts, f"Missing restart policies: {missing_restarts}")


def validate_no_obvious_hardcoded_secrets() -> None:
    files = [
        path
        for path in PROJECT_ROOT.rglob("*")
        if path.is_file()
        and "node_modules" not in path.parts
        and "data" not in path.parts
        and path.name not in {".env", ".env.example"}
        and path.name != "validate_hardening_phase.py"
        and path.suffix in {".py", ".yml", ".yaml", ".sh", ".json", ".properties"}
    ]
    suspicious = []
    patterns = [
        re.compile(r"local-superset-secret-key"),
        re.compile(r"openmetadata_password"),
        re.compile(r"openmetadata_root"),
        re.compile(r"mongodb://[^:{\s]+:[^@{\s]+@"),
    ]
    for path in files:
        text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern in patterns:
            if pattern.search(text):
                suspicious.append(str(path.relative_to(PROJECT_ROOT)))
                break
    assert_ok(not suspicious, f"Obvious hardcoded secret values remain: {suspicious}")


def validate_dashboard_ui_source() -> None:
    content = (PROJECT_ROOT / "dashboard" / "web" / "src" / "main.jsx").read_text(encoding="utf-8")
    for text in [
        "Alerts Center",
        "Audit Logs",
        "User Sessions",
        "Security Overview",
        "Backup & Export",
        "Service Monitoring",
        "Runtime Metrics",
        "Platform Stability",
    ]:
        assert_ok(text in content, f"Dashboard UI is missing {text}")


def run_previous_phase_scripts(logger, skip: bool) -> None:
    if skip:
        logger.info("Skipping previous phase scripts by request")
        return
    if not Path("/.dockerenv").exists():
        logger.info("Skipping container-only previous phase scripts outside Docker")
        return
    for command in PREVIOUS_PHASE_COMMANDS:
        logger.info("Running previous validation: %s", " ".join(command))
        result = subprocess.run(command, capture_output=True, text=True, timeout=1200)
        if result.returncode != 0:
            sys.stdout.write(result.stdout)
            sys.stderr.write(result.stderr)
        assert_ok(result.returncode == 0, f"Previous phase validation failed: {' '.join(command)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-previous-phase-scripts", action="store_true")
    args = parser.parse_args()

    logger = setup_logging("validate_hardening_phase")
    logger.info("Validating Phase 8 production hardening")

    tokens = validate_auth_and_rbac()
    validate_observability(tokens["admin_token"])
    validate_backup(tokens["admin_token"])
    validate_compose_hardening()
    validate_no_obvious_hardcoded_secrets()
    validate_dashboard_ui_source()
    run_previous_phase_scripts(logger, args.skip_previous_phase_scripts)

    logger.info("Phase 8 hardening validation passed")


if __name__ == "__main__":
    main()
