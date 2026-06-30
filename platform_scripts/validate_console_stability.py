#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]


def load_environment() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_environment()

API_BASE = os.environ.get("ONOV8_CONSOLE_API_BASE", "http://localhost:8001").rstrip("/")
WEB_BASE = os.environ.get("ONOV8_CONSOLE_WEB_BASE", "http://localhost:5173").rstrip("/")
USERNAME = os.environ.get("ONOV8_ADMIN_USERNAME", "admin")
PASSWORD = os.environ.get("ONOV8_ADMIN_PASSWORD", "admin")


def fail(message: str) -> None:
    print(f"FAIL: {message}")
    raise SystemExit(1)


def ok(message: str) -> None:
    print(f"OK: {message}")


def request_json(method: str, path: str, token: str | None = None, payload: dict | None = None, *, expect: int = 200, timeout: int = 60) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(f"{API_BASE}{path}", data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            status = response.status
    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        status = exc.code
    except URLError as exc:
        fail(f"{method} {path} could not connect: {exc}")

    if status != expect:
        fail(f"{method} {path} returned HTTP {status}, expected {expect}: {body[:300]}")
    try:
        return json.loads(body or "{}")
    except json.JSONDecodeError as exc:
        fail(f"{method} {path} returned malformed JSON: {exc}")


def request_text(url: str, *, expect: int = 200, timeout: int = 30) -> str:
    try:
        with urlopen(url, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            status = response.status
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        status = exc.code
    except URLError as exc:
        fail(f"{url} could not connect: {exc}")
    if status != expect:
        fail(f"{url} returned HTTP {status}, expected {expect}: {body[:200]}")
    return body


def login() -> str:
    payload = request_json("POST", "/api/auth/login", payload={"username": USERNAME, "password": PASSWORD}, timeout=30)
    token = payload.get("token")
    if not token:
        fail("Login did not return a session token")
    user = payload.get("user") or {}
    if user.get("username") != USERNAME:
        fail(f"Login returned unexpected user: {user}")
    ok("login returns token and user payload")
    return token


def cleanup_test_sources(prefix: str) -> None:
    safe_prefix = prefix.replace("'", "''")
    sql = f"DELETE FROM source_connections WHERE source_name LIKE '{safe_prefix}%'"
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "dashboard-postgres",
        "psql",
        "-U",
        os.environ.get("DASHBOARD_POSTGRES_USER", "dashboard"),
        "-d",
        os.environ.get("DASHBOARD_POSTGRES_DB", "onov8_dashboard"),
        "-c",
        sql,
    ]
    try:
        subprocess.run(command, cwd=ROOT, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        print("WARN: could not hard-delete temporary stability test source records")


def validate_source_crud(token: str) -> None:
    prefix = "stability_test_"
    cleanup_test_sources(prefix)
    source_name = f"{prefix}{uuid.uuid4().hex[:8]}"
    created_id = ""
    try:
        created = request_json(
            "POST",
            "/api/sources",
            token,
            {
                "source_name": source_name,
                "source_type": "mongo",
                "host": "mongo-users",
                "port": 27017,
                "database_name": "users_service",
                "auth_database": "admin",
                "username": os.environ.get("MONGO_ROOT_USERNAME", "admin"),
                "password": os.environ.get("MONGO_ROOT_PASSWORD", "admin123"),
                "include_collections": [],
                "exclude_collections": [],
                "cursor_field": "updatedAt",
                "ingestion_mode": "python",
                "is_active": True,
            },
            timeout=30,
        )
        created_id = created["id"]
        ok("source create returns a stable source record")

        updated = request_json(
            "PUT",
            f"/api/sources/{created_id}",
            token,
            {
                "source_name": source_name,
                "source_type": "mongo",
                "host": "mongo-users",
                "port": 27017,
                "database_name": "users_service",
                "auth_database": "admin",
                "username": os.environ.get("MONGO_ROOT_USERNAME", "admin"),
                "include_collections": ["users"],
                "exclude_collections": [],
                "cursor_field": "updatedAt",
                "ingestion_mode": "python",
                "is_active": True,
            },
            timeout=30,
        )
        if updated.get("include_collections") != ["users"]:
            fail("source update did not persist include_collections")
        ok("source update is stable")

        test = request_json("POST", f"/api/sources/{created_id}/test", token, timeout=30)
        if test.get("status") not in {"ok", "error"} or "source" not in test:
            fail(f"source test returned malformed payload: {test}")
        ok("source connection test returns structured payload")

        discovery = request_json("POST", f"/api/sources/{created_id}/discover", token, timeout=30)
        if not isinstance(discovery.get("collections"), list):
            fail(f"source discovery returned malformed payload: {discovery}")
        ok("source discovery returns collection list")

        deleted = request_json("DELETE", f"/api/sources/{created_id}", token, timeout=30)
        if deleted.get("is_active") is not False:
            fail("source deactivate did not mark source inactive")
        ok("source deactivate is stable")
    finally:
        cleanup_test_sources(prefix)


def validate_core_api_refresh(token: str) -> None:
    endpoints = [
        ("GET", "/api/sources"),
        ("GET", "/api/raw/overview"),
        ("GET", "/api/bronze/overview"),
        ("GET", "/api/silver/overview"),
        ("GET", "/api/query/overview"),
        ("GET", "/api/operations/health"),
        ("GET", "/api/operations/logs"),
        ("GET", "/api/platform/runtime-metrics"),
        ("GET", "/api/platform/stability"),
        ("GET", "/api/services/status"),
    ]
    for method, path in endpoints:
        request_json(method, path, token, timeout=90)
    ok("core dashboard refresh endpoints return valid JSON")


def validate_operations_center(token: str) -> None:
    validation = request_json("POST", "/api/operations/global/validate", token, {"triggered_by": "stability-validator"}, timeout=300)
    if validation.get("status") not in {"ok", "warning", "error"}:
        fail(f"operations validation returned malformed status: {validation}")
    if not isinstance(validation.get("checks"), list):
        fail("operations validation did not return checks list")
    ok("operations center validation returns structured result")


def validate_web_routes() -> None:
    routes = [
        "/",
        "/sources",
        "/operations",
        "/setup-checklist",
        "/setup-wizard/welcome",
        "/setup-wizard/final-readiness",
        "/user-guide",
    ]
    for route in routes:
        html = request_text(f"{WEB_BASE}{route}")
        if "<div id=\"root\"" not in html:
            fail(f"web route {route} did not return the console shell")
    ok("frontend routes reload without server-side 404s")


def validate_auth_lifecycle() -> None:
    token = login()
    me = request_json("GET", "/api/auth/me", token, timeout=30)
    if me.get("user", {}).get("username") != USERNAME:
        fail("/api/auth/me did not preserve authenticated user")
    ok("auth persistence validates current session")

    request_json("GET", "/api/sources", expect=401, timeout=30)
    ok("protected API rejects missing token")

    request_json("POST", "/api/auth/logout", token, timeout=30)
    request_json("GET", "/api/auth/me", token, expect=401, timeout=30)
    ok("logout revokes the current token")

    token = login()
    request_json("GET", "/api/auth/me", token, timeout=30)
    ok("login works again after logout")
    return token


def main() -> None:
    start = time.time()
    token = validate_auth_lifecycle()
    validate_source_crud(token)
    validate_core_api_refresh(token)
    validate_operations_center(token)
    validate_web_routes()
    request_json("POST", "/api/auth/logout", token, timeout=30)
    ok(f"console stability validation passed in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
