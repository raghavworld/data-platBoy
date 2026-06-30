#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from typing import Any

import requests
from psycopg2 import sql
from psycopg2.extras import DictCursor

from common import load_environment, s3_client
from dashboard_db import dashboard_connection, init_dashboard_db
from import_local_mongo_sources import EXCLUDED_DATABASES, TARGET_DATABASES


DEMO_SOURCES = {"users_service", "orders_service", "products_service", "payments_service"}
PROCESSED_TABLES = [
    "raw_collection_run_statuses",
    "raw_batch_fingerprints",
    "raw_files",
    "raw_schema_snapshots",
    "raw_collection_states",
    "raw_ingestion_runs",
    "raw_maintenance_events",
    "bronze_file_states",
    "bronze_collection_states",
    "bronze_schema_snapshots",
    "bronze_column_mappings",
    "bronze_processing_runs",
    "bronze_maintenance_events",
    "silver_batch_fingerprints",
    "silver_collection_states",
    "silver_schema_snapshots",
    "silver_quality_metrics",
    "silver_processing_runs",
    "silver_maintenance_events",
]


def ok(message: str, details: Any | None = None) -> dict[str, Any]:
    return {"status": "ok", "message": message, "details": details}


def fail(message: str, details: Any | None = None) -> None:
    raise RuntimeError(f"{message}: {details}" if details is not None else message)


def existing_tables() -> set[str]:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                """
            )
            return {row[0] for row in cursor.fetchall()}


def table_count(cursor: Any, table_name: str, existing: set[str]) -> int:
    if table_name not in existing:
        return 0
    cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table_name)))
    return int(cursor.fetchone()[0])


def source_rows() -> list[dict[str, Any]]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT * FROM source_connections ORDER BY source_name")
            return [dict(row) for row in cursor.fetchall()]


def validate_sources(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    by_name = {row["source_name"]: row for row in rows}
    demo_present = sorted(DEMO_SOURCES & set(by_name))
    if demo_present:
        fail("Demo source connections are still present", demo_present)
    checks.append(ok("Demo source connections are gone"))

    missing = sorted(set(TARGET_DATABASES) - set(by_name))
    if missing:
        fail("Requested real Mongo source connections are missing", missing)
    checks.append(ok("All requested real Mongo source connections exist", sorted(TARGET_DATABASES)))

    active_real = sorted(row["source_name"] for row in rows if row["source_name"] in TARGET_DATABASES and row["is_active"])
    if active_real:
        fail("Real Mongo source connections must be inactive by default", active_real)
    checks.append(ok("All requested real Mongo sources are inactive"))

    malformed: list[dict[str, Any]] = []
    for name in TARGET_DATABASES:
        row = by_name[name]
        config = row.get("connection_config_json") or {}
        if isinstance(config, str):
            config = json.loads(config)
        problems = []
        if row.get("source_type") != "mongo":
            problems.append("source_type")
        if row.get("database_name") != name:
            problems.append("database_name")
        if config.get("host") != "host.docker.internal":
            problems.append("host")
        if int(config.get("port") or 0) != 27017:
            problems.append("port")
        if row.get("cursor_field") != "AUTO":
            problems.append("cursor_field")
        if row.get("ingestion_mode") != "python":
            problems.append("ingestion_mode")
        if problems:
            malformed.append({"source": name, "problems": problems})
    if malformed:
        fail("Real Mongo source connection settings are incorrect", malformed)
    checks.append(ok("Real Mongo source connection settings are correct"))

    excluded_present = sorted(EXCLUDED_DATABASES & set(by_name))
    if excluded_present:
        fail("Excluded Mongo databases were imported", excluded_present)
    checks.append(ok("Excluded databases were not imported", sorted(EXCLUDED_DATABASES)))
    return checks


def login_headers(api_base: str) -> dict[str, str]:
    username = os.environ.get("ONOV8_ADMIN_USERNAME", "admin")
    password = os.environ.get("ONOV8_ADMIN_PASSWORD", "admin")
    response = requests.post(
        f"{api_base}/api/auth/login",
        json={"username": username, "password": password},
        timeout=5,
    )
    response.raise_for_status()
    token = response.json().get("token")
    return {"Authorization": f"Bearer {token}"} if token else {}


def request_sources(api_base: str) -> tuple[float, list[dict[str, Any]]]:
    started = time.monotonic()
    response = requests.get(f"{api_base}/api/sources", timeout=5)
    if response.status_code == 401:
        response = requests.get(f"{api_base}/api/sources", headers=login_headers(api_base), timeout=5)
    elapsed = time.monotonic() - started
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        fail("Source list API returned a non-list payload", payload)
    return elapsed, payload


def validate_source_api(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    api_base = os.environ.get("DASHBOARD_API_BASE_URL") or f"http://localhost:{os.environ.get('DASHBOARD_API_PORT', '8001')}"
    before = {str(row["id"]): row.get("last_test_at") for row in rows}
    elapsed, payload = request_sources(api_base.rstrip("/"))
    if elapsed > 2.0:
        fail("Source list API is too slow", {"duration_seconds": round(elapsed, 3)})
    after = {str(item["id"]): item.get("last_test_at") for item in payload}
    changed = sorted(source_id for source_id, value in before.items() if after.get(source_id) != value)
    if changed:
        fail("Source refresh changed last test timestamps, which suggests blocking health checks ran", changed)
    return [
        ok("Source list API responds quickly", {"duration_seconds": round(elapsed, 3)}),
        ok("Source refresh did not trigger connection tests"),
    ]


def count_bucket_objects(bucket_name: str, prefix: str = "") -> int:
    client = s3_client()
    total = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        total += len(page.get("Contents", []))
    return total


def validate_processed_state() -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    existing = existing_tables()
    table_counts: dict[str, int] = {}
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for table_name in PROCESSED_TABLES:
                table_counts[table_name] = table_count(cursor, table_name, existing)
    non_empty = {table_name: count for table_name, count in table_counts.items() if count}
    if non_empty:
        fail("Processed RAW/Bronze/Silver metadata remains after wipe", non_empty)
    checks.append(ok("RAW/Bronze/Silver dashboard metadata is empty"))

    raw_objects = count_bucket_objects(os.environ["MINIO_BUCKET_RAW"])
    bronze_objects = count_bucket_objects(os.environ["MINIO_BUCKET_DELTA"], "bronze/")
    silver_objects = count_bucket_objects(os.environ["MINIO_BUCKET_DELTA"], "silver/")
    object_counts = {"raw": raw_objects, "bronze": bronze_objects, "silver": silver_objects}
    if any(object_counts.values()):
        fail("RAW/Bronze/Silver object storage is not empty", object_counts)
    checks.append(ok("RAW/Bronze/Silver object storage is empty", object_counts))
    return checks


def main() -> int:
    load_environment()
    rows = source_rows()
    checks = [
        *validate_sources(rows),
        *validate_source_api(rows),
        *validate_processed_state(),
    ]
    print(json.dumps({"status": "ok", "checks": checks}, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2, sort_keys=True, default=str))
        raise
