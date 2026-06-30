from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import requests

from common import PROJECT_ROOT, setup_logging


REQUIRED_NAV_LABELS = [
    "Overview",
    "Source Connections",
    "Raw Layer",
    "Bronze Layer",
    "Silver Layer",
    "Query Layer",
    "BI / Superset",
    "Governance / Metadata",
    "Operations Center",
    "Alerts Center",
    "Backup & Export",
    "Service Monitoring",
    "Runtime Metrics",
    "Platform Stability",
    "Users",
    "Roles",
    "Permissions",
    "Teams",
    "Ownership",
    "Sessions",
    "API Keys",
    "Settings",
    "Audit Logs",
    "Security Overview",
]

REQUIRED_ADMIN_ENDPOINTS = [
    "/api/admin/users",
    "/api/admin/roles",
    "/api/admin/permissions",
    "/api/admin/teams",
    "/api/admin/ownership",
    "/api/admin/api-keys",
    "/api/admin/settings",
]

REQUIRED_OPERATIONAL_ENDPOINTS = [
    "/api/platform/full-health",
    "/api/platform/alerts",
    "/api/platform/audit-logs",
    "/api/platform/sessions",
    "/api/platform/security-overview",
    "/api/platform/backups",
    "/api/platform/runtime-metrics",
    "/api/platform/stability",
    "/api/operations/health",
    "/api/operations/orchestration",
    "/api/governance/ownership",
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
    response = requests.request(method, f"{api_base()}{path}", headers=headers, timeout=kwargs.pop("timeout", 120), **kwargs)
    if expect_ok:
        response.raise_for_status()
    try:
        payload: Any = response.json()
    except Exception:
        payload = response.text
    return response.status_code, payload


def login(username: str, password: str) -> tuple[str, dict[str, Any]]:
    _, payload = request_json("POST", "/api/auth/login", json={"username": username, "password": password}, timeout=30)
    token = payload.get("token")
    assert_ok(bool(token), f"Login did not return a token for {username}")
    return token, payload.get("user") or {}


def validate_navigation_source() -> None:
    source = (PROJECT_ROOT / "dashboard" / "web" / "src" / "main.jsx").read_text(encoding="utf-8")
    for label in REQUIRED_NAV_LABELS:
        assert_ok(label in source, f"Missing navigation label: {label}")
    for component in [
        "UsersAdminPage",
        "RolesAdminPage",
        "PermissionsAdminPage",
        "TeamsAdminPage",
        "OwnershipAdminPage",
        "ApiKeysAdminPage",
        "SettingsAdminPage",
        "UserSessionsPage",
    ]:
        assert_ok(f"function {component}" in source, f"Missing admin component: {component}")
    assert_ok('section: "Administration"' in source, "Administration navigation group is missing")


def validate_admin_endpoints(admin_token: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for path in REQUIRED_ADMIN_ENDPOINTS:
        _, payload = request_json("GET", path, admin_token, timeout=60)
        payloads[path] = payload
    assert_ok(any(user.get("role") == "admin" for user in payloads["/api/admin/users"]), "No admin user visible")
    roles = {role.get("role") for role in payloads["/api/admin/roles"]}
    assert_ok({"admin", "operator", "viewer"}.issubset(roles), f"Missing roles: {roles}")
    settings = {setting.get("setting_key") for setting in payloads["/api/admin/settings"]}
    assert_ok("environment.profile" in settings and "alerts.slow_query_ms" in settings, "Core platform settings are missing")
    return payloads


def validate_user_team_session_and_key_flows(admin_token: str) -> None:
    username = "validation_viewer"
    password = f"validation-pass-{int(time.time())}"
    create_status, create_payload = request_json(
        "POST",
        "/api/admin/users",
        admin_token,
        expect_ok=False,
        json={
            "username": username,
            "password": password,
            "role": "viewer",
            "display_name": "Validation Viewer",
            "status": "active",
        },
        timeout=30,
    )
    if create_status == 409:
        request_json("PATCH", f"/api/admin/users/{username}", admin_token, json={"role": "viewer", "status": "active"}, timeout=30)
        request_json("POST", f"/api/admin/users/{username}/reset-password", admin_token, json={"password": password}, timeout=30)
    else:
        assert_ok(create_status == 200 and create_payload.get("status") == "ok", f"User create failed: {create_status} {create_payload}")

    viewer_token, viewer = login(username, password)
    assert_ok(viewer.get("role") == "viewer", f"Validation user role mismatch: {viewer}")
    denied, _ = request_json("GET", "/api/admin/users", viewer_token, expect_ok=False, timeout=30)
    assert_ok(denied == 403, f"Viewer could access admin users: HTTP {denied}")

    team_payload = {
        "team_name": "Platform Validation",
        "domain": "platform",
        "owner_username": username,
        "description": "Validation team",
        "status": "active",
    }
    _, team_result = request_json("POST", "/api/admin/teams", admin_token, json=team_payload, timeout=30)
    team = team_result.get("team") or {}
    assert_ok(team.get("id"), f"Team create/upsert failed: {team_result}")
    request_json("POST", f"/api/admin/teams/{team['id']}/members", admin_token, json={"username": username, "role_in_team": "steward"}, timeout=30)

    _, ownership = request_json(
        "POST",
        "/api/admin/ownership",
        admin_token,
        json={"asset_name": "validation.asset", "asset_owner": username, "team": team["team_name"], "domain": "platform", "status": "assigned"},
        timeout=30,
    )
    assert_ok(ownership.get("ownership", {}).get("asset_owner") == username, f"Ownership assignment failed: {ownership}")

    _, key_result = request_json(
        "POST",
        "/api/admin/api-keys",
        admin_token,
        json={"key_name": "Validation Key", "owner_username": username, "role": "viewer", "expires_in_days": 1},
        timeout=30,
    )
    api_token = key_result.get("token")
    api_key = key_result.get("api_key") or {}
    assert_ok(api_token and api_key.get("id"), f"API key create failed: {key_result}")
    _, sources = request_json("GET", "/api/sources", api_token, timeout=30)
    assert_ok(isinstance(sources, list), "API key could not read sources")
    request_json("POST", f"/api/admin/api-keys/{api_key['id']}/revoke", admin_token, timeout=30)

    session_id = viewer.get("session_id")
    assert_ok(bool(session_id), "Validation viewer session id missing")
    request_json("POST", f"/api/admin/sessions/{session_id}/revoke", admin_token, timeout=30)
    revoked_status, _ = request_json("GET", "/api/auth/me", viewer_token, expect_ok=False, timeout=30)
    assert_ok(revoked_status == 401, f"Revoked session still worked: HTTP {revoked_status}")

    request_json("POST", f"/api/admin/users/{username}/disable", admin_token, timeout=30)


def validate_operational_sections(admin_token: str) -> None:
    for path in REQUIRED_OPERATIONAL_ENDPOINTS:
        status, payload = request_json("GET", path, admin_token, timeout=120)
        assert_ok(status == 200, f"Operational endpoint failed: {path}")
        assert_ok(payload is not None, f"Operational endpoint returned empty payload: {path}")


def validate_docs() -> None:
    doc = PROJECT_ROOT / "docs" / "platform_gap_review.md"
    assert_ok(doc.exists(), "docs/platform_gap_review.md is missing")
    text = doc.read_text(encoding="utf-8")
    for heading in ["Critical Missing Features", "Important Improvements", "Nice-To-Have Future Ideas", "Implemented Immediately"]:
        assert_ok(heading in text, f"Gap review missing section: {heading}")


def main() -> None:
    logger = setup_logging("validate_platform_completeness")
    logger.info("Validating platform UX and feature completeness")
    admin_username = os.environ["ONOV8_ADMIN_USERNAME"]
    admin_password = os.environ["ONOV8_ADMIN_PASSWORD"]
    admin_token, admin_user = login(admin_username, admin_password)
    assert_ok(admin_user.get("role") == "admin", f"Configured admin is not admin: {admin_user}")
    validate_navigation_source()
    validate_admin_endpoints(admin_token)
    validate_user_team_session_and_key_flows(admin_token)
    validate_operational_sections(admin_token)
    validate_docs()
    logger.info("Platform completeness validation passed")


if __name__ == "__main__":
    main()
