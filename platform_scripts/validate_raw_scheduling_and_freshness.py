#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from psycopg2.extras import Json

from common import load_environment
from dashboard_db import dashboard_connection, init_dashboard_db, new_id


API_BASE = os.environ.get("DASHBOARD_API_BASE_URL") or f"http://localhost:{os.environ.get('DASHBOARD_API_PORT', '8001')}"


def assert_ok(condition: bool, message: str, details: Any | None = None) -> None:
    if not condition:
        raise RuntimeError(f"{message}: {details}" if details is not None else message)


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{API_BASE.rstrip('/')}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    response.raise_for_status()
    return response.json()


def create_validation_source(suffix: str) -> str:
    source_id = new_id()
    collections = ["due_orders", "manual_archive", "custom_events", "stale_metrics", "never_ingested"]
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO source_connections (
                    id, source_name, source_type, database_name, auth_database,
                    connection_config_json, include_collections_json, exclude_collections_json,
                    cursor_field, ingestion_mode, is_active, last_test_status, last_test_message
                )
                VALUES (%s, %s, 'mongo', %s, 'admin', %s, %s, '[]'::jsonb,
                        'updatedAt', 'python', true, 'ok', 'raw scheduling validation')
                """,
                (
                    source_id,
                    f"raw_schedule_validation_{suffix}",
                    f"raw_schedule_db_{suffix}",
                    Json({"host": "example.invalid", "port": 27017}),
                    Json(collections),
                ),
            )
            for index, collection_name in enumerate(collections, start=1):
                cursor.execute(
                    """
                    INSERT INTO source_connection_collections (
                        id, source_id, collection_name, is_active, record_count,
                        estimated_size_bytes, detected_cursor_field, detected_cursor_strategy
                    )
                    VALUES (%s, %s, %s, true, %s, %s, 'updatedAt', 'incremental_timestamp')
                    """,
                    (new_id(), source_id, collection_name, index * 100, index * 2048),
                )
    return source_id


def seed_state(source_id: str, database_name: str, collection_name: str, last_success_delta: timedelta, latest_delta: timedelta | None = None) -> None:
    now = datetime.now(timezone.utc)
    latest_source_document_at = None if latest_delta is None else now - latest_delta
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_collection_states (
                    id, source_id, database_name, collection_name, cursor_field,
                    configured_cursor_field, detected_cursor_field, ingestion_strategy,
                    last_cursor_value, last_success_at, last_row_count, last_raw_path,
                    latest_source_document_at, freshness_checked_at
                )
                VALUES (%s, %s, %s, %s, 'updatedAt', 'updatedAt', 'updatedAt',
                        'incremental_timestamp', %s, %s, 100, NULL, %s, now())
                ON CONFLICT (source_id, database_name, collection_name)
                DO UPDATE SET
                    last_success_at = EXCLUDED.last_success_at,
                    latest_source_document_at = EXCLUDED.latest_source_document_at,
                    updated_at = now()
                """,
                (
                    new_id(),
                    source_id,
                    database_name,
                    collection_name,
                    json.dumps({"kind": "timestamp", "field": "updatedAt", "value": (latest_source_document_at or now).isoformat()}),
                    now - last_success_delta,
                    latest_source_document_at,
                ),
            )


def force_due(source_id: str, collection_name: str | None = None) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            if collection_name:
                cursor.execute(
                    """
                    UPDATE source_connection_collections
                    SET next_scheduled_run_at = now() - interval '5 minutes'
                    WHERE source_id = %s AND collection_name = %s
                    """,
                    (source_id, collection_name),
                )
            else:
                cursor.execute(
                    """
                    UPDATE source_connections
                    SET next_scheduled_run_at = now() - interval '5 minutes'
                    WHERE id = %s
                    """,
                    (source_id,),
                )


def cleanup(source_id: str) -> None:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for table in (
                "raw_collection_run_statuses",
                "raw_files",
                "raw_collection_states",
                "raw_batch_fingerprints",
                "raw_schema_snapshots",
                "raw_ingestion_batches",
                "raw_run_database_progress",
            ):
                cursor.execute(f"DELETE FROM {table} WHERE source_id = %s", (source_id,))
            cursor.execute("DELETE FROM source_connections WHERE id = %s", (source_id,))


def main() -> int:
    load_environment()
    init_dashboard_db()
    suffix = new_id().replace("-", "")[:10]
    source_id = create_validation_source(suffix)
    database_name = f"raw_schedule_db_{suffix}"
    checks: list[dict[str, Any]] = []
    try:
        source_schedule = request_json(
            "POST",
            f"/api/raw/source/{source_id}/schedule",
            json={"schedule_type": "hourly", "interval_hours": 1},
            timeout=20,
        )
        assert_ok(source_schedule["schedule"]["next_scheduled_run_at"], "Source schedule did not calculate next run", source_schedule)
        checks.append({"status": "ok", "message": "database schedule persists and calculates next run"})

        manual_override = request_json(
            "POST",
            f"/api/raw/source/{source_id}/collection/manual_archive/schedule",
            json={"schedule_type": "manual_only"},
            timeout=20,
        )
        assert_ok(
            manual_override["schedule"]["effective_schedule"]["schedule_type"] == "manual_only",
            "Collection manual schedule did not override database schedule",
            manual_override,
        )
        checks.append({"status": "ok", "message": "collection manual_only schedule overrides database schedule"})

        collection_schedule = request_json(
            "POST",
            f"/api/raw/source/{source_id}/collection/custom_events/schedule",
            json={"schedule_type": "hourly", "interval_hours": 2},
            timeout=20,
        )
        assert_ok(
            collection_schedule["schedule"]["effective_schedule"]["level"] == "collection",
            "Collection schedule was not marked as collection-level override",
            collection_schedule,
        )
        checks.append({"status": "ok", "message": "collection schedule override persists"})

        seed_state(source_id, database_name, "due_orders", timedelta(minutes=30), timedelta(minutes=25))
        seed_state(source_id, database_name, "custom_events", timedelta(hours=8), timedelta(hours=1))
        seed_state(source_id, database_name, "stale_metrics", timedelta(hours=30), timedelta(hours=1))
        force_due(source_id)
        force_due(source_id, "custom_events")

        freshness = request_json("GET", f"/api/raw/freshness/{source_id}", timeout=20)
        statuses = {item["collection_name"]: item["freshness_status"] for item in freshness.get("items", [])}
        assert_ok(statuses.get("custom_events") == "delayed", "Delayed collection was not visible", statuses)
        assert_ok(statuses.get("stale_metrics") == "stale", "Stale collection was not detected", statuses)
        assert_ok(statuses.get("never_ingested") == "never_ingested", "Never-ingested collection was not detected", statuses)
        checks.append({"status": "ok", "message": "freshness API detects delayed, stale, and never-ingested collections"})

        due = request_json("GET", "/api/raw/schedule/due", timeout=20)
        due_names = {item["collection_name"] for item in due.get("due_collections", []) if item.get("source_id") == source_id}
        assert_ok("due_orders" in due_names, "Inherited due database schedule was not returned", due)
        assert_ok("custom_events" in due_names, "Collection-level due schedule was not returned", due)
        assert_ok("manual_archive" not in due_names, "manual_only collection was not skipped by scheduler", due)
        checks.append({"status": "ok", "message": "scheduler due API respects active collections and manual_only overrides"})

        print(json.dumps({"status": "ok", "checks": checks}, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        cleanup(source_id)


if __name__ == "__main__":
    raise SystemExit(main())
