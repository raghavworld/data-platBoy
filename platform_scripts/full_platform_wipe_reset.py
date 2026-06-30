#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import shutil
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg2
import pymongo
import requests
from psycopg2 import sql
from psycopg2.extras import DictCursor, Json

from common import DATA_DIR, LOG_DIR, STATE_DIR, load_environment, mongo_client, mongo_sources, s3_client, trino_connection
from dashboard_db import dashboard_connection, init_dashboard_db, new_id


FIRST_CONFIRMATION = "FULL PLATFORM WIPE"
SECOND_CONFIRMATION = "RESET EVERYTHING"
ONBOARDING_STORAGE_KEY = "onov8_onboarding_progress_v1"
FULL_WIPE_EMPTY_START_SETTING = "onboarding.full_wipe_empty_start"
CONTAINER_PROJECT_NAME = os.environ.get("COMPOSE_PROJECT_NAME", "local-data-platform-modular")


def platform_container(service_name: str) -> str:
    return f"{CONTAINER_PROJECT_NAME}-{service_name}"

AIRFLOW_DAGS = [
    "raw_ingestion_pipeline",
    "bronze_processing_pipeline",
    "silver_processing_pipeline",
    "governance_metadata_pipeline",
    "bi_validation_pipeline",
]

PIPELINE_STOP_CONTAINERS = [
    platform_container("airflow-scheduler"),
    platform_container("airflow-webserver"),
]

RESET_STOP_CONTAINERS = [
    platform_container("spark"),
    platform_container("trino"),
    platform_container("hive-metastore"),
    platform_container("superset"),
    platform_container("openmetadata"),
    platform_container("metadata-db"),
    platform_container("metadata-search"),
]

RESET_START_ORDER = [
    platform_container("metadata-db"),
    platform_container("metadata-search"),
    platform_container("hive-metastore"),
    platform_container("spark"),
    platform_container("trino"),
    platform_container("airflow-webserver"),
    platform_container("airflow-scheduler"),
    platform_container("superset"),
    platform_container("openmetadata"),
]

RAW_TABLES = [
    "raw_collection_run_statuses",
    "raw_run_events",
    "raw_run_timings",
    "raw_run_database_progress",
    "raw_ingestion_batches",
    "raw_batch_fingerprints",
    "raw_files",
    "raw_schema_snapshots",
    "raw_collection_states",
    "raw_ingestion_runs",
    "raw_maintenance_events",
]

BRONZE_TABLES = [
    "bronze_run_events",
    "bronze_file_states",
    "bronze_collection_states",
    "bronze_schema_snapshots",
    "bronze_column_mappings",
    "bronze_processing_runs",
    "bronze_maintenance_events",
]

SILVER_TABLES = [
    "silver_run_events",
    "silver_processing_batches",
    "silver_batch_fingerprints",
    "silver_collection_states",
    "silver_schema_snapshots",
    "silver_quality_metrics",
    "silver_transformation_quality",
    "silver_field_profiles",
    "silver_transform_plans",
    "silver_pii_audit",
    "silver_child_table_audit",
    "silver_raw_json_fallback_audit",
    "silver_processing_runs",
    "silver_maintenance_events",
]

QUERY_TABLES = [
    "query_safe_views",
    "query_validation_runs",
    "query_history",
]

BI_TABLES = [
    "bi_datasets",
    "bi_dashboards",
    "bi_charts",
    "bi_validation_runs",
    "bi_query_failures",
    "bi_dashboard_loads",
]

GOVERNANCE_TABLES = [
    "governance_assets",
    "governance_lineage_edges",
    "governance_pii_tags",
    "governance_sync_runs",
    "governance_ownership",
]

OPERATIONS_TABLES = [
    "end_to_end_flow_items",
    "end_to_end_flow_steps",
    "end_to_end_flow_runs",
    "service_health_checks",
    "operations_history",
    "platform_events",
    "platform_alerts",
    "audit_logs",
    "replay_jobs",
    "service_restart_history",
    "global_validation_runs",
]

SOURCE_TABLES = [
    "source_connections",
]

ADMIN_STATE_TABLES = [
    "platform_team_memberships",
    "platform_api_keys",
    "platform_teams",
    "platform_settings",
    "user_sessions",
    "backup_exports",
    "platform_users",
]

DASHBOARD_WIPE_TABLES = [
    *SOURCE_TABLES,
    *RAW_TABLES,
    *BRONZE_TABLES,
    *SILVER_TABLES,
    *QUERY_TABLES,
    *BI_TABLES,
    *GOVERNANCE_TABLES,
    *OPERATIONS_TABLES,
    *ADMIN_STATE_TABLES,
]

DEMO_SOURCE_NAMES = {
    "users_service",
    "orders_service",
    "products_service",
    "payments_service",
}

DEMO_MONGO_CONTAINERS = [
    platform_container("mongo-users"),
    platform_container("mongo-orders"),
    platform_container("mongo-products"),
    platform_container("mongo-payments"),
    platform_container("mongo-express"),
]

DEMO_MONGO_DATA_DIRS = [
    DATA_DIR / "mongo-users",
    DATA_DIR / "mongo-orders",
    DATA_DIR / "mongo-products",
    DATA_DIR / "mongo-payments",
]

SOURCE_CONNECTION_COLUMNS = [
    "id",
    "source_name",
    "source_type",
    "database_name",
    "auth_database",
    "connection_config_json",
    "secret_reference",
    "include_collections_json",
    "exclude_collections_json",
    "cursor_field",
    "ingestion_mode",
    "is_active",
    "last_test_status",
    "last_test_message",
    "last_test_at",
    "created_at",
    "updated_at",
]

METADATA_ESTIMATE_TABLES = [
    "raw_files",
    "raw_collection_states",
    "raw_schema_snapshots",
    "raw_batch_fingerprints",
    "raw_ingestion_batches",
    "raw_run_database_progress",
    "bronze_file_states",
    "bronze_collection_states",
    "bronze_schema_snapshots",
    "bronze_column_mappings",
    "silver_collection_states",
    "silver_batch_fingerprints",
    "silver_schema_snapshots",
    "silver_quality_metrics",
    "silver_transformation_quality",
    "silver_field_profiles",
    "silver_transform_plans",
    "silver_pii_audit",
    "silver_child_table_audit",
    "silver_raw_json_fallback_audit",
    "query_safe_views",
    "bi_datasets",
    "bi_dashboards",
    "bi_charts",
    "governance_assets",
    "governance_lineage_edges",
    "governance_pii_tags",
    "governance_ownership",
]

LOG_ESTIMATE_TABLES = [
    "raw_collection_run_statuses",
    "raw_run_events",
    "raw_run_timings",
    "raw_run_database_progress",
    "raw_ingestion_batches",
    "raw_ingestion_runs",
    "raw_maintenance_events",
    "bronze_run_events",
    "bronze_processing_runs",
    "bronze_maintenance_events",
    "silver_run_events",
    "silver_processing_batches",
    "silver_processing_runs",
    "silver_maintenance_events",
    "query_history",
    "query_validation_runs",
    "bi_validation_runs",
    "bi_charts",
    "bi_query_failures",
    "bi_dashboard_loads",
    "governance_sync_runs",
    "end_to_end_flow_items",
    "end_to_end_flow_steps",
    "end_to_end_flow_runs",
    "operations_history",
    "platform_events",
    "platform_alerts",
    "audit_logs",
    "replay_jobs",
    "service_restart_history",
    "global_validation_runs",
    "service_health_checks",
]

DEFAULT_PLATFORM_SETTINGS = {
    "environment.profile": ("environment", "local", "Environment profile shown in the console"),
    "alerts.slow_query_ms": ("observability", 5000, "Slow query threshold in milliseconds"),
    "alerts.rebuilds_per_hour": ("observability", 3, "Rebuild warning threshold per hour"),
    "retention.audit_days": ("retention", 90, "Audit log retention target in days"),
    "retention.alert_days": ("retention", 90, "Alert retention target in days"),
    "backups.default_type": ("backup", "metadata_snapshot", "Default backup export type"),
    "operations.require_typed_confirmation": ("safety", True, "Typed confirmations for destructive operations"),
    "operations.replay_default_dry_run": ("safety", True, "Default replay operations to dry-run"),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def existing_dashboard_tables() -> set[str]:
    init_dashboard_db()
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


def dashboard_table_count(table_name: str) -> int:
    try:
        if table_name not in existing_dashboard_tables():
            return 0
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table_name)))
                return int(cursor.fetchone()[0])
    except Exception:
        return 0


def dashboard_table_counts(table_names: Iterable[str]) -> dict[str, int]:
    existing = existing_dashboard_tables()
    counts: dict[str, int] = {}
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for table_name in table_names:
                if table_name not in existing:
                    counts[table_name] = 0
                    continue
                cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table_name)))
                counts[table_name] = int(cursor.fetchone()[0])
    return counts


def snapshot_source_connections() -> list[dict[str, Any]]:
    """Keep source connection records outside the blast radius of the wipe."""
    if "source_connections" not in existing_dashboard_tables():
        return []
    columns_sql = sql.SQL(", ").join(sql.Identifier(column) for column in SOURCE_CONNECTION_COLUMNS)
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                sql.SQL("SELECT {} FROM source_connections ORDER BY source_name").format(columns_sql)
            )
            snapshot: list[dict[str, Any]] = []
            for row in cursor.fetchall():
                item = dict(row)
                item["id"] = str(item["id"])
                snapshot.append(item)
            return snapshot


def restore_source_connections(snapshot: list[dict[str, Any]]) -> dict[str, Any]:
    if not snapshot:
        return {"status": "skipped", "rows_restored": 0}
    init_dashboard_db()
    insert_columns = sql.SQL(", ").join(sql.Identifier(column) for column in SOURCE_CONNECTION_COLUMNS)
    values = sql.SQL(", ").join(sql.Placeholder(column) for column in SOURCE_CONNECTION_COLUMNS)
    update_columns = [column for column in SOURCE_CONNECTION_COLUMNS if column not in {"source_name", "created_at"}]
    update_assignments = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
        for column in update_columns
    )
    query = sql.SQL(
        """
        INSERT INTO source_connections ({columns})
        VALUES ({values})
        ON CONFLICT (source_name) DO UPDATE SET
            {updates}
        """
    ).format(columns=insert_columns, values=values, updates=update_assignments)

    restored = 0
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for row in snapshot:
                payload = {
                    **row,
                    "connection_config_json": Json(row.get("connection_config_json") or {}),
                    "include_collections_json": Json(row.get("include_collections_json") or []),
                    "exclude_collections_json": Json(row.get("exclude_collections_json") or []),
                }
                cursor.execute(query, payload)
                restored += 1
    return {"status": "ok", "rows_restored": restored}


def count_bucket_objects(bucket_name: str, prefix: str = "") -> dict[str, int]:
    client = s3_client()
    object_count = 0
    storage_bytes = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for item in page.get("Contents", []):
            object_count += 1
            storage_bytes += int(item.get("Size", 0))
    return {"object_count": object_count, "storage_bytes": storage_bytes}


def safe_count_bucket_objects(bucket_name: str | None, prefix: str = "") -> dict[str, Any]:
    if not bucket_name:
        return {"object_count": 0, "storage_bytes": 0, "status": "missing"}
    try:
        return {**count_bucket_objects(bucket_name, prefix), "status": "ok"}
    except Exception as exc:
        return {"object_count": 0, "storage_bytes": 0, "status": "error", "message": str(exc)}


def local_generated_artifact_count() -> int:
    paths = [
        STATE_DIR,
        LOG_DIR,
        DATA_DIR / "backups",
        DATA_DIR / "storage",
        DATA_DIR / "compute",
        DATA_DIR / "bi",
        DATA_DIR / "governance",
        DATA_DIR / "spark-warehouse",
        DATA_DIR / "superset_home",
        DATA_DIR / "metadata-es",
        DATA_DIR / "metadata-mysql",
    ]
    count = 0
    for path in paths:
        if not path.exists():
            continue
        count += sum(1 for item in path.rglob("*") if item.is_file())
    return count


def estimate_full_platform_wipe() -> dict[str, Any]:
    load_environment()
    counts = dashboard_table_counts([*METADATA_ESTIMATE_TABLES, *LOG_ESTIMATE_TABLES])
    raw_bucket = safe_count_bucket_objects(os.environ.get("MINIO_BUCKET_RAW"))
    delta_bucket = os.environ.get("MINIO_BUCKET_DELTA")
    bronze_bucket = safe_count_bucket_objects(delta_bucket, "bronze/")
    silver_bucket = safe_count_bucket_objects(delta_bucket, "silver/")
    audit_bucket = safe_count_bucket_objects(os.environ.get("MINIO_BUCKET_AUDIT"))
    raw_files = max(counts.get("raw_files", 0), int(raw_bucket.get("object_count", 0)))
    return {
        "status": "ok",
        "checked_at": utc_now_iso(),
        "raw_files": raw_files,
        "bronze_tables": counts.get("bronze_collection_states", 0),
        "silver_tables": counts.get("silver_collection_states", 0),
        "dashboards": counts.get("bi_dashboards", 0),
        "metadata_rows": sum(counts.get(table_name, 0) for table_name in METADATA_ESTIMATE_TABLES),
        "log_rows": sum(counts.get(table_name, 0) for table_name in LOG_ESTIMATE_TABLES),
        "source_connections_to_delete": dashboard_table_count("source_connections"),
        "source_connections_preserved": 0,
        "object_storage": {
            "raw": raw_bucket,
            "bronze": bronze_bucket,
            "silver": silver_bucket,
            "audit": audit_bucket,
        },
        "local_artifacts": local_generated_artifact_count(),
    }


def docker_socket_request(method: str, path: str, timeout_seconds: float = 120.0) -> tuple[int, str]:
    socket_path = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
    if not os.path.exists(socket_path):
        raise RuntimeError("Docker socket is not available")
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(timeout_seconds)
    try:
        client.connect(socket_path)
        request = f"{method} {path} HTTP/1.1\r\nHost: docker\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        client.sendall(request.encode("utf-8"))
        chunks = []
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        client.close()
    raw = b"".join(chunks)
    header, _, body = raw.partition(b"\r\n\r\n")
    header_text = header.decode("latin-1", errors="replace") if header else ""
    status_line = header_text.splitlines()[0] if header_text else ""
    parts = status_line.split()
    status_code = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 0
    if "transfer-encoding: chunked" in header_text.lower():
        body = decode_http_chunked_body(body)
    return status_code, body.decode("utf-8", errors="replace")


def decode_http_chunked_body(body: bytes) -> bytes:
    decoded = bytearray()
    index = 0
    while index < len(body):
        line_end = body.find(b"\r\n", index)
        if line_end == -1:
            break
        size_text = body[index:line_end].split(b";", 1)[0].strip()
        try:
            size = int(size_text, 16)
        except ValueError:
            return body
        index = line_end + 2
        if size == 0:
            break
        decoded.extend(body[index:index + size])
        index += size + 2
    return bytes(decoded)


def docker_inspect(container_name: str) -> dict[str, Any] | None:
    try:
        status, body = docker_socket_request("GET", f"/containers/{container_name}/json")
        if status >= 400:
            return None
        return json.loads(body)
    except Exception:
        return None


def docker_running(container_name: str) -> bool:
    inspect = docker_inspect(container_name)
    return bool(inspect and inspect.get("State", {}).get("Running"))


def stop_container(container_name: str) -> dict[str, Any]:
    inspect = docker_inspect(container_name)
    if not inspect:
        return {"container_name": container_name, "status": "missing"}
    if not inspect.get("State", {}).get("Running"):
        return {"container_name": container_name, "status": "already_stopped"}
    try:
        status, body = docker_socket_request("POST", f"/containers/{container_name}/stop?t=45", timeout_seconds=120)
    except TimeoutError as exc:
        time.sleep(2)
        if not docker_running(container_name):
            return {
                "container_name": container_name,
                "status": "ok",
                "message": "Docker stop request timed out after the container stopped.",
            }
        return {"container_name": container_name, "status": "error", "message": str(exc)}
    return {
        "container_name": container_name,
        "status": "ok" if status in {200, 204, 304} else "error",
        "http_status": status,
        "message": body.strip(),
    }


def remove_container(container_name: str) -> dict[str, Any]:
    inspect = docker_inspect(container_name)
    if not inspect:
        return {"container_name": container_name, "status": "missing"}
    status, body = docker_socket_request("DELETE", f"/containers/{container_name}?force=true&v=true")
    return {
        "container_name": container_name,
        "status": "ok" if status in {200, 204} else "error",
        "http_status": status,
        "message": body.strip(),
    }


def start_container(container_name: str) -> dict[str, Any]:
    inspect = docker_inspect(container_name)
    if not inspect:
        return {"container_name": container_name, "status": "missing"}
    if inspect.get("State", {}).get("Running"):
        return {"container_name": container_name, "status": "already_running"}
    status, body = docker_socket_request("POST", f"/containers/{container_name}/start")
    return {
        "container_name": container_name,
        "status": "ok" if status in {200, 204, 304} else "error",
        "http_status": status,
        "message": body.strip(),
    }


def pause_airflow_dags(paused: bool) -> list[dict[str, Any]]:
    airflow_url = os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080").rstrip("/")
    auth = (os.environ.get("AIRFLOW_USER", "admin"), os.environ.get("AIRFLOW_PASSWORD", "admin"))
    results = []
    for dag_id in AIRFLOW_DAGS:
        try:
            response = requests.patch(
                f"{airflow_url}/api/v1/dags/{dag_id}",
                params={"update_mask": "is_paused"},
                auth=auth,
                json={"is_paused": paused},
                timeout=10,
            )
            results.append(
                {
                    "dag_id": dag_id,
                    "status": "ok" if response.ok else "error",
                    "http_status": response.status_code,
                }
            )
        except Exception as exc:
            results.append({"dag_id": dag_id, "status": "error", "message": str(exc)})
    return results


def stop_pipelines_safely() -> dict[str, Any]:
    return {
        "paused_dags": pause_airflow_dags(True),
        "stopped_containers": [stop_container(name) for name in PIPELINE_STOP_CONTAINERS],
    }


def stop_reset_services() -> list[dict[str, Any]]:
    return [stop_container(name) for name in RESET_STOP_CONTAINERS]


def remove_demo_mongo_sources() -> dict[str, Any]:
    stopped = [stop_container(name) for name in DEMO_MONGO_CONTAINERS]
    removed = [remove_container(name) for name in DEMO_MONGO_CONTAINERS]
    cleared = [remove_path_contents(path) for path in DEMO_MONGO_DATA_DIRS]
    return {"status": "ok", "stopped_containers": stopped, "removed_containers": removed, "paths": cleared}


def start_required_services() -> dict[str, Any]:
    started = [start_container(name) for name in RESET_START_ORDER]
    time.sleep(2)
    unpaused = pause_airflow_dags(False)
    return {"started_containers": started, "unpaused_dags": unpaused}


def ensure_bucket(bucket_name: str) -> None:
    client = s3_client()
    buckets = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    if bucket_name not in buckets:
        client.create_bucket(Bucket=bucket_name)


def clear_bucket(bucket_name: str, prefix: str = "") -> dict[str, Any]:
    client = s3_client()
    ensure_bucket(bucket_name)
    objects_deleted = 0
    bytes_deleted = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        bytes_deleted += sum(int(item.get("Size", 0)) for item in objects)
        for index in range(0, len(objects), 1000):
            chunk = objects[index:index + 1000]
            client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": [{"Key": item["Key"]} for item in chunk]},
            )
            objects_deleted += len(chunk)
    return {"bucket": bucket_name, "prefix": prefix, "objects_deleted": objects_deleted, "bytes_deleted": bytes_deleted}


def clear_minio_layers() -> dict[str, Any]:
    client = s3_client()
    configured = [
        os.environ["MINIO_BUCKET_RAW"],
        os.environ["MINIO_BUCKET_DELTA"],
        os.environ.get("MINIO_BUCKET_AUDIT", ""),
    ]
    try:
        bucket_names = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    except Exception:
        bucket_names = set()
    audit_buckets = sorted({name for name in [*configured, *bucket_names] if name and "audit" in name.lower()})
    results = {
        "raw": clear_bucket(os.environ["MINIO_BUCKET_RAW"]),
        "delta": clear_bucket(os.environ["MINIO_BUCKET_DELTA"]),
        "audit": [clear_bucket(bucket_name) for bucket_name in audit_buckets],
    }
    return {"status": "ok", "buckets": results}


def drop_trino_schema_objects(schema_name: str) -> dict[str, Any]:
    dropped_tables = 0
    dropped_views = 0
    errors: list[str] = []
    connection = None
    cursor = None
    try:
        connection = trino_connection(schema=schema_name)
        cursor = connection.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS delta.{schema_name}")
        try:
            cursor.execute(f"SHOW VIEWS FROM delta.{schema_name}")
            views = [row[0] for row in cursor.fetchall()]
        except Exception:
            views = []
        for view_name in views:
            try:
                cursor.execute(f'DROP VIEW IF EXISTS delta.{schema_name}."{view_name}"')
                dropped_views += 1
            except Exception as exc:
                errors.append(f"view {view_name}: {exc}")
        try:
            cursor.execute(f"SHOW TABLES FROM delta.{schema_name}")
            tables = [row[0] for row in cursor.fetchall()]
        except Exception:
            tables = []
        for table_name in tables:
            try:
                cursor.execute(f'DROP TABLE IF EXISTS delta.{schema_name}."{table_name}"')
                dropped_tables += 1
            except Exception as exc:
                errors.append(f"table {table_name}: {exc}")
    except Exception as exc:
        errors.append(str(exc))
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
    return {
        "schema": schema_name,
        "dropped_tables": dropped_tables,
        "dropped_views": dropped_views,
        "status": "ok" if not errors else "warning",
        "errors": errors,
    }


def clear_trino_metadata() -> dict[str, Any]:
    return {"status": "ok", "schemas": [drop_trino_schema_objects("silver"), drop_trino_schema_objects("bronze")]}


def postgres_config(dbname: str) -> dict[str, Any]:
    return {
        "host": os.environ.get("POSTGRES_HOST", "airflow-postgres"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": dbname,
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "connect_timeout": 10,
    }


def postgres_tables(connection) -> set[str]:
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


def truncate_postgres_tables(connection, table_names: Iterable[str]) -> int:
    existing = sorted(set(table_names) & postgres_tables(connection))
    if not existing:
        return 0
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
                sql.SQL(", ").join(sql.Identifier(table_name) for table_name in existing)
            )
        )
    return len(existing)


def clear_airflow_history() -> dict[str, Any]:
    tables = [
        "callback_request",
        "celery_taskmeta",
        "celery_tasksetmeta",
        "dag_run",
        "dag_tag",
        "dag_warning",
        "dataset_dag_run_queue",
        "dataset_event",
        "import_error",
        "job",
        "log",
        "rendered_task_instance_fields",
        "serialized_dag",
        "sla_miss",
        "task_fail",
        "task_instance",
        "task_reschedule",
        "xcom",
    ]
    try:
        with psycopg2.connect(**postgres_config(os.environ["AIRFLOW_DB_NAME"])) as connection:
            truncated = truncate_postgres_tables(connection, tables)
        return {"status": "ok", "tables_truncated": truncated}
    except Exception as exc:
        return {"status": "warning", "message": str(exc), "tables_truncated": 0}


def clear_superset_assets() -> dict[str, Any]:
    asset_tables = [
        "alerts",
        "annotation",
        "annotation_layer",
        "cache_keys",
        "css_templates",
        "dashboard_roles",
        "dashboard_slices",
        "dashboard_user",
        "dashboards",
        "dbs",
        "dynamic_plugin",
        "embedded_dashboards",
        "favstar",
        "key_value",
        "logs",
        "query",
        "report_execution_log",
        "report_recipient",
        "report_schedule",
        "rls_filter_roles",
        "row_level_security_filters",
        "saved_query",
        "sl_columns",
        "slices",
        "sql_metrics",
        "tab_state",
        "table_columns",
        "table_schema",
        "tables",
        "tag",
        "tagged_object",
        "url",
    ]
    try:
        with psycopg2.connect(**postgres_config(os.environ["SUPERSET_DB_NAME"])) as connection:
            truncated = truncate_postgres_tables(connection, asset_tables)
        return {"status": "ok", "tables_truncated": truncated}
    except Exception as exc:
        return {"status": "warning", "message": str(exc), "tables_truncated": 0}


def clear_hive_metastore() -> dict[str, Any]:
    try:
        with psycopg2.connect(**postgres_config(os.environ["METASTORE_DB_NAME"])) as connection:
            existing = postgres_tables(connection)
            keep = {
                "VERSION",
                "version",
                "SCHEMA_VERSION",
                "schema_version",
                "SEQUENCE_TABLE",
                "sequence_table",
                "CTLGS",
                "ctlgs",
            }
            tables = [table_name for table_name in existing if table_name not in keep]
            truncated = truncate_postgres_tables(connection, tables)
            ensure_hive_metastore_default_catalog(connection)
        return {"status": "ok", "tables_truncated": truncated, "default_catalog": "hive"}
    except Exception as exc:
        return {"status": "warning", "message": str(exc), "tables_truncated": 0}


def ensure_hive_metastore_default_catalog(connection) -> None:
    if "CTLGS" not in postgres_tables(connection):
        return
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO "CTLGS" ("CTLG_ID", "NAME", "DESC", "LOCATION_URI", "CREATE_TIME")
            VALUES (1, 'hive', 'Default catalog for Hive', 's3a://edp-delta/', EXTRACT(EPOCH FROM now())::int)
            ON CONFLICT DO NOTHING
            """
        )


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    iterations = 200_000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    return f"pbkdf2_sha256${iterations}${salt}${digest.hex()}"


def seed_platform_defaults() -> dict[str, Any]:
    env_users: dict[str, dict[str, str]] = {}
    for prefix, username, role in [
        ("ONOV8_ADMIN", "admin", "admin"),
        ("ONOV8_OPERATOR", "operator", "operator"),
        ("ONOV8_VIEWER", "viewer", "viewer"),
    ]:
        resolved_username = os.environ.get(f"{prefix}_USERNAME", username)
        password = os.environ.get(f"{prefix}_PASSWORD")
        if password:
            env_users[resolved_username] = {"password": password, "role": os.environ.get(f"{prefix}_ROLE", role)}

    settings = {
        **DEFAULT_PLATFORM_SETTINGS,
        FULL_WIPE_EMPTY_START_SETTING: (
            "onboarding",
            True,
            "Skip automatic demo source seeding after a full platform wipe so onboarding starts empty",
        ),
    }

    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            for username, user in env_users.items():
                cursor.execute(
                    """
                    INSERT INTO platform_users (
                        username, display_name, role, status, password_hash,
                        password_changed_at, source, created_by
                    )
                    VALUES (%s, %s, %s, 'active', %s, now(), 'env', 'full_platform_wipe_reset')
                    ON CONFLICT (username) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        role = EXCLUDED.role,
                        status = 'active',
                        password_hash = EXCLUDED.password_hash,
                        password_changed_at = now(),
                        source = 'env',
                        created_by = 'full_platform_wipe_reset',
                        updated_at = now()
                    """,
                    (username, username, user["role"], hash_password(user["password"])),
                )
            for key, (category, value, description) in settings.items():
                cursor.execute(
                    """
                    INSERT INTO platform_settings (setting_key, category, setting_value_json, description, updated_by)
                    VALUES (%s, %s, %s, %s, 'full_platform_wipe_reset')
                    ON CONFLICT (setting_key) DO UPDATE SET
                        category = EXCLUDED.category,
                        setting_value_json = EXCLUDED.setting_value_json,
                        description = EXCLUDED.description,
                        updated_by = 'full_platform_wipe_reset',
                        updated_at = now()
                    """,
                    (key, category, Json(value), description),
                )
    return {"status": "ok", "users_seeded": len(env_users), "settings_seeded": len(settings)}


def clear_dashboard_operational_db() -> dict[str, Any]:
    init_dashboard_db()
    existing = existing_dashboard_tables()
    to_truncate = sorted(set(DASHBOARD_WIPE_TABLES) & existing)
    if to_truncate:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
                        sql.SQL(", ").join(sql.Identifier(table_name) for table_name in to_truncate)
                    )
                )
    defaults = seed_platform_defaults()
    return {"status": "ok", "tables_truncated": len(to_truncate), "defaults": defaults}


def clear_postgres_operational_state() -> dict[str, Any]:
    return {
        "status": "ok",
        "dashboard": clear_dashboard_operational_db(),
        "airflow": clear_airflow_history(),
        "superset": clear_superset_assets(),
        "hive_metastore": clear_hive_metastore(),
    }


def remove_path_contents(path: Path, preserve_names: set[str] | None = None) -> dict[str, Any]:
    preserve_names = preserve_names or set()
    removed_files = 0
    removed_dirs = 0
    errors: list[str] = []
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return {"path": str(path), "removed_files": 0, "removed_dirs": 0, "errors": []}
    for item in list(path.iterdir()):
        if item.name in preserve_names:
            continue
        try:
            if item.is_dir():
                file_count = sum(1 for child in item.rglob("*") if child.is_file())
                shutil.rmtree(item)
                removed_files += file_count
                removed_dirs += 1
            else:
                item.unlink()
                removed_files += 1
        except OSError as exc:
            errors.append(f"{item}: {exc}")
    path.mkdir(parents=True, exist_ok=True)
    return {"path": str(path), "removed_files": removed_files, "removed_dirs": removed_dirs, "errors": errors}


def clear_local_generated_artifacts() -> dict[str, Any]:
    paths = [
        STATE_DIR,
        LOG_DIR,
        DATA_DIR / "backups",
        DATA_DIR / "storage",
        DATA_DIR / "compute" / "logs",
        DATA_DIR / "compute" / "spark-warehouse",
        DATA_DIR / "bi" / "superset_home",
        DATA_DIR / "spark-warehouse",
        DATA_DIR / "superset_home",
    ]
    pycache_roots = [
        Path(__file__).resolve().parent / "__pycache__",
        Path(__file__).resolve().parents[1] / "dashboard" / "api" / "app" / "__pycache__",
        Path(__file__).resolve().parents[1] / "airflow" / "dags" / "__pycache__",
    ]
    results = []
    for path in paths:
        results.append(remove_path_contents(path))
    results.extend(remove_path_contents(path) for path in pycache_roots if path.exists())
    return {"status": "ok", "paths": results}


def clear_openmetadata_data() -> dict[str, Any]:
    stopped = [stop_container(name) for name in [platform_container("openmetadata"), platform_container("metadata-db"), platform_container("metadata-search")]]
    paths = [
        DATA_DIR / "governance" / "metadata-db",
        DATA_DIR / "governance" / "metadata-search",
        DATA_DIR / "metadata-mysql",
        DATA_DIR / "metadata-es",
    ]
    cleared = [remove_path_contents(path) for path in paths]
    return {"status": "ok", "stopped_containers": stopped, "paths": cleared}


def mongo_source_counts() -> dict[str, Any]:
    load_environment()
    counts: list[dict[str, Any]] = []
    total = 0
    for source in mongo_sources():
        try:
            with mongo_client(source.service_name) as client:
                count = int(client[source.database_name][source.collection_name].estimated_document_count())
            total += count
            counts.append(
                {
                    "service_name": source.service_name,
                    "database_name": source.database_name,
                    "collection_name": source.collection_name,
                    "documents": count,
                    "status": "ok",
                }
            )
        except Exception as exc:
            counts.append(
                {
                    "service_name": source.service_name,
                    "database_name": source.database_name,
                    "collection_name": source.collection_name,
                    "documents": 0,
                    "status": "error",
                    "message": str(exc),
                }
            )
    return {"total_documents": total, "sources": counts}


def assert_mongo_preserved(before: dict[str, Any], after: dict[str, Any]) -> None:
    before_counts = {
        (item["service_name"], item["database_name"], item["collection_name"]): int(item.get("documents") or 0)
        for item in before.get("sources", [])
        if item.get("status") == "ok"
    }
    after_counts = {
        (item["service_name"], item["database_name"], item["collection_name"]): int(item.get("documents") or 0)
        for item in after.get("sources", [])
        if item.get("status") == "ok"
    }
    for key, before_count in before_counts.items():
        if after_counts.get(key) != before_count:
            raise RuntimeError(f"Mongo source data changed for {key}: before={before_count} after={after_counts.get(key)}")


def assert_source_connections_preserved(before_count: int, after_count: int) -> None:
    if before_count != after_count:
        raise RuntimeError(f"Source connections changed during wipe: before={before_count} after={after_count}")


def run_full_platform_wipe(triggered_by: str = "dashboard") -> dict[str, Any]:
    load_environment()
    started_at = time.monotonic()
    started_wall = utc_now_iso()
    before_estimate = estimate_full_platform_wipe()
    source_connections_before = dashboard_table_count("source_connections")

    steps: list[dict[str, Any]] = []
    steps.append({"step": "stop_pipelines_safely", "result": stop_pipelines_safely()})
    steps.append({"step": "remove_demo_mongo_sources", "result": remove_demo_mongo_sources()})
    steps.append({"step": "clear_trino_metadata", "result": clear_trino_metadata()})
    steps.append({"step": "stop_reset_services", "result": stop_reset_services()})
    steps.append({"step": "clear_minio_processed_layers", "result": clear_minio_layers()})
    steps.append({"step": "clear_openmetadata_metadata", "result": clear_openmetadata_data()})
    steps.append({"step": "clear_postgres_operational_state", "result": clear_postgres_operational_state()})
    steps.append({"step": "clear_runtime_states_validations_logs_onboarding", "result": clear_local_generated_artifacts()})
    steps.append({"step": "restart_required_services", "result": start_required_services()})
    steps.append(
        {
            "step": "finalize_fresh_dashboard_state",
            "result": {
                "dashboard": clear_dashboard_operational_db(),
                "demo_mongo_sources": remove_demo_mongo_sources(),
                "local_artifacts": clear_local_generated_artifacts(),
            },
        }
    )

    after_estimate = estimate_full_platform_wipe()
    source_connections_after = int(after_estimate.get("source_connections_to_delete") or 0)
    return {
        "status": "ok",
        "message": "Full platform wipe completed; platform state, demo sources, and processed data were cleared. Local host MongoDB data was not modified.",
        "triggered_by": triggered_by,
        "started_at": started_wall,
        "finished_at": utc_now_iso(),
        "duration_seconds": round(time.monotonic() - started_at, 2),
        "confirmations": [FIRST_CONFIRMATION, SECOND_CONFIRMATION],
        "estimated_deleted": before_estimate,
        "post_wipe_estimate": after_estimate,
        "source_connections_before": source_connections_before,
        "source_connections_after": source_connections_after,
        "onboarding": {
            "storage_key": ONBOARDING_STORAGE_KEY,
            "reset_to_step": "welcome",
            "client_action": "remove_local_storage_and_open_setup_wizard",
        },
        "steps": steps,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Wipe ONOV8 platform state while preserving Mongo source data.")
    parser.add_argument("--confirmation")
    parser.add_argument("--second-confirmation")
    parser.add_argument("--estimate", action="store_true")
    parser.add_argument("--triggered-by", default="make")
    args = parser.parse_args()

    if args.estimate:
        print(json.dumps(estimate_full_platform_wipe(), indent=2, default=str, sort_keys=True))
        return 0

    if args.confirmation != FIRST_CONFIRMATION or args.second_confirmation != SECOND_CONFIRMATION:
        print(
            f"Confirmation required: --confirmation {FIRST_CONFIRMATION!r} "
            f"--second-confirmation {SECOND_CONFIRMATION!r}",
            flush=True,
        )
        return 2

    result = run_full_platform_wipe(args.triggered_by)
    print(json.dumps(result, indent=2, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
