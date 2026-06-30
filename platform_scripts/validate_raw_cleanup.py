#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from typing import Any

import requests
from psycopg2.extras import Json

from common import load_environment, s3_client
from dashboard_db import dashboard_connection, ensure_raw_run, init_dashboard_db, new_id


API_BASE = os.environ.get("DASHBOARD_API_BASE_URL") or f"http://localhost:{os.environ.get('DASHBOARD_API_PORT', '8001')}"


def assert_ok(condition: bool, message: str, details: Any | None = None) -> None:
    if not condition:
        raise RuntimeError(f"{message}: {details}" if details is not None else message)


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{API_BASE.rstrip('/')}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    response.raise_for_status()
    return response.json()


def create_source(source_name: str, database_name: str, collections: list[str]) -> str:
    source_id = new_id()
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
                        'updatedAt', 'python', true, 'ok', 'raw cleanup validation')
                """,
                (
                    source_id,
                    source_name,
                    database_name,
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
                    (new_id(), source_id, collection_name, index * 10, index * 1024),
                )
    return source_id


def seed_raw_metadata(source_id: str, database_name: str, collection_name: str, run_id: str, suffix: str) -> str:
    bucket = os.environ["MINIO_BUCKET_RAW"]
    object_key = f"python/{database_name}/{collection_name}/dt=2026-05-13/hr=12/batch_cleanup_{suffix}.jsonl.gz"
    payload = b'{"cleanup":true}\n'
    s3_client().put_object(Bucket=bucket, Key=object_key, Body=payload)
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_files (
                    id, run_id, source_id, database_name, collection_name,
                    minio_bucket, object_key, row_count, file_size_bytes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s)
                """,
                (new_id(), run_id, source_id, database_name, collection_name, bucket, object_key, len(payload)),
            )
            cursor.execute(
                """
                INSERT INTO raw_collection_states (
                    id, source_id, database_name, collection_name, cursor_field,
                    configured_cursor_field, detected_cursor_field, ingestion_strategy,
                    last_cursor_value, last_success_at, last_row_count, last_raw_path
                )
                VALUES (%s, %s, %s, %s, 'updatedAt', 'updatedAt', 'updatedAt',
                        'incremental_timestamp', 'cursor-1', now(), 1, %s)
                ON CONFLICT (source_id, database_name, collection_name)
                DO UPDATE SET last_raw_path = EXCLUDED.last_raw_path, updated_at = now()
                """,
                (new_id(), source_id, database_name, collection_name, f"s3://{bucket}/{object_key}"),
            )
            cursor.execute(
                """
                INSERT INTO raw_collection_run_statuses (
                    id, run_id, source_id, database_name, collection_name,
                    status, rows_found, rows_written, raw_object_key,
                    previous_cursor_value, new_cursor_value, cursor_strategy, finished_at
                )
                VALUES (%s, %s, %s, %s, %s, 'success', 1, 1, %s,
                        NULL, 'cursor-1', 'incremental_timestamp', now())
                """,
                (new_id(), run_id, source_id, database_name, collection_name, object_key),
            )
            cursor.execute(
                """
                INSERT INTO raw_ingestion_batches (
                    batch_id, run_id, source_id, database_name, collection_name,
                    batch_number, total_batches, estimated_rows, actual_rows,
                    processed_rows, status, raw_object_key
                )
                VALUES (%s, %s, %s, %s, %s, 1, 1, 1, 1, 1, 'success', %s)
                """,
                (new_id(), run_id, source_id, database_name, collection_name, object_key),
            )
            cursor.execute(
                """
                INSERT INTO raw_batch_fingerprints (
                    id, source_id, database_name, collection_name, cursor_field,
                    min_cursor_value, max_cursor_value, row_count, batch_checksum, raw_object_key
                )
                VALUES (%s, %s, %s, %s, 'updatedAt', 'cursor-0', 'cursor-1', 1, %s, %s)
                """,
                (new_id(), source_id, database_name, collection_name, f"checksum-{suffix}", object_key),
            )
            cursor.execute(
                """
                INSERT INTO raw_schema_snapshots (
                    id, source_id, database_name, collection_name, schema_hash,
                    fields_json, new_fields_json, removed_fields_json, change_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, '[]'::jsonb, '[]'::jsonb, 'initial')
                """,
                (new_id(), source_id, database_name, collection_name, f"schema-{suffix}", Json({"fields": ["_id", "updatedAt"]})),
            )
    return object_key


def raw_file_count(source_id: str, collection_name: str | None = None) -> int:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            if collection_name:
                cursor.execute("SELECT count(*) FROM raw_files WHERE source_id = %s AND collection_name = %s", (source_id, collection_name))
            else:
                cursor.execute("SELECT count(*) FROM raw_files WHERE source_id = %s", (source_id,))
            return int(cursor.fetchone()[0])


def metadata_count(table: str, source_id: str, collection_name: str | None = None) -> int:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            if collection_name:
                cursor.execute(f"SELECT count(*) FROM {table} WHERE source_id = %s AND collection_name = %s", (source_id, collection_name))
            else:
                cursor.execute(f"SELECT count(*) FROM {table} WHERE source_id = %s", (source_id,))
            return int(cursor.fetchone()[0])


def object_exists(object_key: str) -> bool:
    try:
        s3_client().head_object(Bucket=os.environ["MINIO_BUCKET_RAW"], Key=object_key)
        return True
    except Exception:
        return False


def source_snapshot(source_id: str) -> dict[str, Any]:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT is_active FROM source_connections WHERE id = %s", (source_id,))
            source_row = cursor.fetchone()
            cursor.execute(
                """
                SELECT collection_name, is_active
                FROM source_connection_collections
                WHERE source_id = %s
                ORDER BY collection_name
                """,
                (source_id,),
            )
            collections = {row[0]: bool(row[1]) for row in cursor.fetchall()}
    return {"exists": source_row is not None, "is_active": bool(source_row[0]) if source_row else None, "collections": collections}


def cleanup(source_ids: list[str], object_keys: list[str], run_ids: list[str]) -> None:
    client = s3_client()
    bucket = os.environ["MINIO_BUCKET_RAW"]
    for object_key in object_keys:
        try:
            client.delete_object(Bucket=bucket, Key=object_key)
        except Exception:
            pass
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
                cursor.execute(f"DELETE FROM {table} WHERE source_id = ANY(%s::uuid[])", (source_ids,))
            if run_ids:
                cursor.execute("DELETE FROM raw_ingestion_runs WHERE id = ANY(%s::uuid[])", (run_ids,))
            cursor.execute("DELETE FROM source_connections WHERE id = ANY(%s::uuid[])", (source_ids,))


def main() -> int:
    load_environment()
    init_dashboard_db()
    suffix = new_id().replace("-", "")[:10]
    source_ids: list[str] = []
    object_keys: list[str] = []
    run_ids: list[str] = []
    checks: list[dict[str, Any]] = []
    try:
        source_a = create_source(f"raw_cleanup_a_{suffix}", f"raw_cleanup_db_a_{suffix}", ["orders", "customers"])
        source_b = create_source(f"raw_cleanup_b_{suffix}", f"raw_cleanup_db_b_{suffix}", ["orders"])
        source_ids.extend([source_a, source_b])
        run_a = ensure_raw_run(f"raw_cleanup_validation_a_{suffix}", triggered_by="validation")
        run_b = ensure_raw_run(f"raw_cleanup_validation_b_{suffix}", triggered_by="validation")
        run_ids.extend([run_a, run_b])
        orders_key = seed_raw_metadata(source_a, f"raw_cleanup_db_a_{suffix}", "orders", run_a, f"{suffix}_orders")
        customers_key = seed_raw_metadata(source_a, f"raw_cleanup_db_a_{suffix}", "customers", run_a, f"{suffix}_customers")
        other_key = seed_raw_metadata(source_b, f"raw_cleanup_db_b_{suffix}", "orders", run_b, f"{suffix}_other")
        object_keys.extend([orders_key, customers_key, other_key])

        result = request_json(
            "POST",
            f"/api/raw/source/{source_a}/collection/orders/delete-raw",
            json={"confirmation": "DELETE RAW COLLECTION", "deactivate": True},
            timeout=30,
        )
        assert_ok(result.get("objects_deleted") >= 1, "Collection RAW delete did not delete any objects", result)
        assert_ok(raw_file_count(source_a, "orders") == 0, "Collection RAW file metadata was not deleted")
        assert_ok(raw_file_count(source_a, "customers") == 1, "Collection RAW delete touched a sibling collection")
        assert_ok(raw_file_count(source_b, "orders") == 1, "Collection RAW delete touched another database")
        assert_ok(not object_exists(orders_key), "Collection RAW object still exists")
        assert_ok(object_exists(customers_key), "Sibling collection RAW object was deleted")
        for table in ("raw_collection_states", "raw_collection_run_statuses", "raw_batch_fingerprints", "raw_schema_snapshots", "raw_ingestion_batches"):
            assert_ok(metadata_count(table, source_a, "orders") == 0, f"{table} still has deleted collection rows")
        snapshot = source_snapshot(source_a)
        assert_ok(snapshot["exists"], "Source Connection was deleted during collection cleanup")
        assert_ok(snapshot["collections"].get("orders") is False, "Deleted collection was not deactivated")
        assert_ok(snapshot["collections"].get("customers") is True, "Sibling collection was deactivated unexpectedly")
        checks.append({"status": "ok", "message": "deleting collection RAW removes only that collection and deactivates it when requested"})

        result = request_json(
            "POST",
            f"/api/raw/source/{source_a}/delete-raw",
            json={"confirmation": "DELETE RAW DATABASE", "deactivate": True},
            timeout=30,
        )
        assert_ok(result.get("objects_deleted") >= 1, "Database RAW delete did not delete remaining database objects", result)
        assert_ok(raw_file_count(source_a) == 0, "Database RAW file metadata was not deleted")
        assert_ok(raw_file_count(source_b) == 1, "Database RAW delete touched another source")
        assert_ok(not object_exists(customers_key), "Database RAW object still exists")
        assert_ok(object_exists(other_key), "Other database RAW object was deleted")
        for table in ("raw_collection_states", "raw_collection_run_statuses", "raw_batch_fingerprints", "raw_schema_snapshots", "raw_ingestion_batches"):
            assert_ok(metadata_count(table, source_a) == 0, f"{table} still has deleted database rows")
        snapshot_a = source_snapshot(source_a)
        snapshot_b = source_snapshot(source_b)
        assert_ok(snapshot_a["exists"], "Source Connection was removed during database cleanup")
        assert_ok(snapshot_a["is_active"] is False, "Database cleanup did not deactivate source when requested")
        assert_ok(all(active is False for active in snapshot_a["collections"].values()), "Database cleanup did not deactivate all collections")
        assert_ok(snapshot_b["exists"] and snapshot_b["is_active"] is True, "Other source was changed by database cleanup")
        assert_ok(snapshot_b["collections"].get("orders") is True, "Other source collection was changed by database cleanup")
        checks.append({"status": "ok", "message": "deleting database RAW removes only that database and preserves Source Connection"})

        print(json.dumps({"status": "ok", "checks": checks}, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        cleanup(source_ids, object_keys, run_ids)


if __name__ == "__main__":
    raise SystemExit(main())
