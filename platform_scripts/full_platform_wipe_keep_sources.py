#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor, Json

from common import DATA_DIR, LOG_DIR, STATE_DIR, load_environment
from dashboard_db import dashboard_connection, init_dashboard_db
from full_platform_wipe_reset import (
    AIRFLOW_DAGS,
    BI_TABLES,
    BRONZE_TABLES,
    GOVERNANCE_TABLES,
    ONBOARDING_STORAGE_KEY,
    QUERY_TABLES,
    RAW_TABLES,
    RESET_START_ORDER,
    SILVER_TABLES,
    clear_airflow_history,
    clear_hive_metastore,
    clear_openmetadata_data,
    clear_superset_assets,
    clear_trino_metadata,
    count_bucket_objects,
    dashboard_table_count,
    dashboard_table_counts,
    docker_running,
    pause_airflow_dags,
    platform_container,
    postgres_config,
    remove_path_contents,
    start_container,
    stop_pipelines_safely,
    stop_reset_services,
)


FULL_WIPE_KEEP_SOURCES_CONFIRMATION = "FULL WIPE KEEP SOURCES"
FULL_WIPE_EMPTY_START_SETTING = "onboarding.full_wipe_empty_start"

PRESERVED_DASHBOARD_TABLES = {
    "source_connections",
    "source_connection_collections",
    "platform_users",
    "platform_team_memberships",
    "platform_api_keys",
    "platform_teams",
    "platform_settings",
    "alembic_version",
    "schema_migrations",
}

ADMIN_RUNTIME_TABLES = [
    "user_sessions",
    "backup_exports",
]

VALIDATION_RECONCILIATION_TABLES = [
    "global_validation_runs",
    "query_validation_runs",
    "bi_validation_runs",
]

MONITORING_TABLES = [
    "platform_alerts",
]

SERVICE_HEALTH_HISTORY_TABLES = [
    "service_health_checks",
]

SERVICE_RESTART_HISTORY_TABLES = [
    "service_restart_history",
]

OPERATIONS_HISTORY_TABLES = [
    "operations_history",
    "end_to_end_flow_items",
    "end_to_end_flow_steps",
    "end_to_end_flow_runs",
]

REPLAY_QUEUE_TABLES = [
    "replay_jobs",
]

LOG_TABLES = [
    "audit_logs",
    "platform_events",
]

ONBOARDING_CACHE_RUNTIME_TABLES = [
    "backup_exports",
]

DASHBOARD_TABLE_GROUPS = {
    "raw": RAW_TABLES,
    "bronze": [*BRONZE_TABLES, "bronze_file_attempts"],
    "silver": SILVER_TABLES,
    "query": QUERY_TABLES,
    "bi": BI_TABLES,
    "governance": GOVERNANCE_TABLES,
    "logs": LOG_TABLES,
    "monitoring": MONITORING_TABLES,
    "operations_history": OPERATIONS_HISTORY_TABLES,
    "replay_queue": REPLAY_QUEUE_TABLES,
    "service_health_history": SERVICE_HEALTH_HISTORY_TABLES,
    "service_restart_history": SERVICE_RESTART_HISTORY_TABLES,
    "validation_reconciliation": VALIDATION_RECONCILIATION_TABLES,
    "onboarding_cache_runtime": ONBOARDING_CACHE_RUNTIME_TABLES,
}

DASHBOARD_KEEP_SOURCE_WIPE_TABLES = sorted(
    set().union(*DASHBOARD_TABLE_GROUPS.values(), ADMIN_RUNTIME_TABLES)
)

LOCAL_GENERATED_ARTIFACT_PATHS = [
    STATE_DIR,
    LOG_DIR,
    DATA_DIR / "airflow" / "logs",
    DATA_DIR / "backups",
    DATA_DIR / "compute" / "logs",
    DATA_DIR / "compute" / "spark-warehouse",
    DATA_DIR / "bi" / "superset_home",
    DATA_DIR / "spark-warehouse",
    DATA_DIR / "superset_home",
]

POST_WIPE_ZERO_ESTIMATE_FIELDS = [
    "metadata_rows",
    "monitoring_rows",
    "validation_reconciliation_rows",
    "runtime_rows",
    "log_rows",
    "replay_queue_count",
    "operations_history_rows",
    "service_health_history_rows",
    "service_restart_history_rows",
]

METADATA_ROW_ESTIMATE_TABLES = sorted(
    table_name
    for table_name in DASHBOARD_KEEP_SOURCE_WIPE_TABLES
    if table_name
    not in {
        "audit_logs",
        "platform_alerts",
        "platform_settings",
        "service_health_checks",
        "service_restart_history",
        "operations_history",
        "platform_events",
        "replay_jobs",
        "user_sessions",
        "backup_exports",
    }
)

ESTIMATE_EXCLUDED_BOOT_TABLES = {
    *PRESERVED_DASHBOARD_TABLES,
    "user_sessions",
}

SOURCE_PRESERVED_SIGNATURE_COLUMNS = [
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
    "ingestion_schedule_type",
    "ingestion_schedule_cron",
    "schedule_enabled",
]

SOURCE_CLEARED_CACHE_COLUMNS = [
    "last_test_status",
    "last_test_message",
    "last_test_at",
    "last_inventory_status",
    "last_inventory_message",
    "last_inventory_at",
    "last_scheduled_run_at",
    "next_scheduled_run_at",
]

SOURCE_CONFIG_CACHE_KEYS = {
    "discovered_collections_count",
    "last_discovered_at",
}

SOURCE_COLLECTION_PRESERVED_SIGNATURE_COLUMNS = [
    "source_id",
    "collection_name",
    "is_active",
    "ingestion_schedule_type",
    "ingestion_schedule_cron",
    "schedule_enabled",
]

SOURCE_COLLECTION_CLEARED_CACHE_COLUMNS = [
    "record_count",
    "estimated_size_bytes",
    "avg_object_size",
    "last_stats_at",
    "detected_cursor_field",
    "detected_cursor_strategy",
    "sample_schema_json",
    "last_error",
    "last_scheduled_run_at",
    "next_scheduled_run_at",
]


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


def clearable_dashboard_tables(existing: set[str] | None = None) -> list[str]:
    existing = existing if existing is not None else existing_dashboard_tables()
    return sorted(table_name for table_name in existing if table_name not in PRESERVED_DASHBOARD_TABLES)


def json_stable(value: Any) -> str:
    return json.dumps(value or ([] if isinstance(value, list) else {}), sort_keys=True, default=str)


def source_config_for_signature(value: Any) -> str:
    config = json.loads(json_stable(value))
    if isinstance(config, dict):
        for key in SOURCE_CONFIG_CACHE_KEYS:
            config.pop(key, None)
    return json_stable(config)


def source_config_cache_keys(value: Any) -> list[str]:
    config = json.loads(json_stable(value))
    if not isinstance(config, dict):
        return []
    return sorted(key for key in SOURCE_CONFIG_CACHE_KEYS if key in config)


def source_connection_signatures() -> list[dict[str, Any]]:
    if "source_connections" not in existing_dashboard_tables():
        return []
    selected_columns = ["is_active", *SOURCE_PRESERVED_SIGNATURE_COLUMNS, *SOURCE_CLEARED_CACHE_COLUMNS]
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in selected_columns)
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(sql.SQL("SELECT id, {} FROM source_connections ORDER BY source_name").format(columns))
            rows: list[dict[str, Any]] = []
            for row in cursor.fetchall():
                item = dict(row)
                item["id"] = str(item["id"])
                item["connection_config_cache_keys"] = source_config_cache_keys(item.get("connection_config_json"))
                item["connection_config_json"] = source_config_for_signature(item.get("connection_config_json"))
                for key in ("include_collections_json", "exclude_collections_json"):
                    item[key] = json_stable(item.get(key))
                rows.append(item)
            return rows


def source_collection_signatures() -> list[dict[str, Any]]:
    if "source_connection_collections" not in existing_dashboard_tables():
        return []
    selected_columns = [*SOURCE_COLLECTION_PRESERVED_SIGNATURE_COLUMNS, *SOURCE_COLLECTION_CLEARED_CACHE_COLUMNS]
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in selected_columns)
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                sql.SQL("SELECT id, {} FROM source_connection_collections ORDER BY source_id, collection_name").format(columns)
            )
            rows: list[dict[str, Any]] = []
            for row in cursor.fetchall():
                item = dict(row)
                item["id"] = str(item["id"])
                item["source_id"] = str(item["source_id"])
                item["sample_schema_json"] = json_stable(item.get("sample_schema_json"))
                rows.append(item)
            return rows


def assert_source_connections_preserved(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> None:
    if not before:
        raise RuntimeError("No source_connections exist to preserve")
    before_signature = [{key: row.get(key) for key in ["id", *SOURCE_PRESERVED_SIGNATURE_COLUMNS]} for row in before]
    after_signature = [{key: row.get(key) for key in ["id", *SOURCE_PRESERVED_SIGNATURE_COLUMNS]} for row in after]
    if before_signature != after_signature:
        raise RuntimeError("source connection credentials/config changed during wipe")
    active_after = [row["source_name"] for row in after if row.get("is_active")]
    if active_after:
        raise RuntimeError(f"Source connections still active after wipe: {active_after}")
    stale_cache = [
        row["source_name"]
        for row in after
        if row.get("connection_config_cache_keys")
        or any(row.get(column) for column in SOURCE_CLEARED_CACHE_COLUMNS)
    ]
    if stale_cache:
        raise RuntimeError(f"Source connection cache fields remain after wipe: {stale_cache}")


def assert_source_collections_preserved(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> None:
    before_signature = [
        {key: row.get(key) for key in ["id", *SOURCE_COLLECTION_PRESERVED_SIGNATURE_COLUMNS]}
        for row in before
    ]
    after_signature = [
        {key: row.get(key) for key in ["id", *SOURCE_COLLECTION_PRESERVED_SIGNATURE_COLUMNS]}
        for row in after
    ]
    if before_signature != after_signature:
        raise RuntimeError("source connection collections changed during wipe")
    stale_cache = [
        f"{row.get('source_id')}:{row.get('collection_name')}"
        for row in after
        if int(row.get("record_count") or 0)
        or int(row.get("estimated_size_bytes") or 0)
        or row.get("avg_object_size")
        or row.get("last_stats_at")
        or row.get("detected_cursor_field")
        or row.get("detected_cursor_strategy")
        or json.loads(row.get("sample_schema_json") or "{}")
        or row.get("last_error")
        or row.get("last_scheduled_run_at")
        or row.get("next_scheduled_run_at")
    ]
    if stale_cache:
        raise RuntimeError(f"Source collection cache fields remain after wipe: {stale_cache[:10]}")


def deactivate_source_connections() -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                UPDATE source_connections
                SET is_active = false,
                    connection_config_json = connection_config_json
                        - 'discovered_collections_count'
                        - 'last_discovered_at',
                    last_test_status = NULL,
                    last_test_message = NULL,
                    last_test_at = NULL,
                    last_inventory_status = NULL,
                    last_inventory_message = NULL,
                    last_inventory_at = NULL,
                    last_scheduled_run_at = NULL,
                    next_scheduled_run_at = NULL,
                    updated_at = now()
                RETURNING source_name
                """
            )
            rows = [row["source_name"] for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'source_connection_collections'
                """
            )
            collection_columns = {row["column_name"] for row in cursor.fetchall()}
            collection_rows: list[str] = []
            if collection_columns:
                reset_expressions = {
                    "record_count": "0",
                    "estimated_size_bytes": "0",
                    "avg_object_size": "NULL",
                    "last_stats_at": "NULL",
                    "detected_cursor_field": "NULL",
                    "detected_cursor_strategy": "NULL",
                    "sample_schema_json": "'{}'::jsonb",
                    "last_error": "NULL",
                    "last_scheduled_run_at": "NULL",
                    "next_scheduled_run_at": "NULL",
                    "updated_at": "now()",
                }
                assignments = [
                    sql.SQL("{} = {}").format(sql.Identifier(column), sql.SQL(expression))
                    for column, expression in reset_expressions.items()
                    if column in collection_columns
                ]
                if assignments:
                    cursor.execute(
                        sql.SQL("UPDATE source_connection_collections SET {} RETURNING collection_name").format(
                            sql.SQL(", ").join(assignments)
                        )
                    )
                    collection_rows = [row["collection_name"] for row in cursor.fetchall()]
    return {
        "status": "ok",
        "sources_deactivated": len(rows),
        "source_names": rows,
        "collections_cache_reset": len(collection_rows),
        "collection_selections_preserved": True,
    }


def clear_dashboard_state_keep_sources() -> dict[str, Any]:
    init_dashboard_db()
    existing = existing_dashboard_tables()
    to_truncate = clearable_dashboard_tables(existing)
    deleted_row_counts: dict[str, int] = {}
    if to_truncate:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                for table_name in to_truncate:
                    cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table_name)))
                    deleted_row_counts[table_name] = int(cursor.fetchone()[0])
                cursor.execute(
                    sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
                        sql.SQL(", ").join(sql.Identifier(table_name) for table_name in to_truncate)
                    )
                )
    inactive = deactivate_source_connections()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO platform_settings (setting_key, category, setting_value_json, description, updated_by)
                VALUES (%s, 'onboarding', %s, %s, 'full_platform_wipe_keep_sources')
                ON CONFLICT (setting_key) DO UPDATE SET
                    category = EXCLUDED.category,
                    setting_value_json = EXCLUDED.setting_value_json,
                    description = EXCLUDED.description,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = now()
                """,
                (
                    FULL_WIPE_EMPTY_START_SETTING,
                    Json(True),
                    "Skip automatic source seeding after a keep-sources full wipe; sources remain inactive until an admin re-enables them.",
                ),
            )
    return {
        "status": "ok",
        "tables_truncated": len(to_truncate),
        "rows_deleted": sum(deleted_row_counts.values()),
        "deleted_row_counts": deleted_row_counts,
        "preserved_tables": sorted(existing & PRESERVED_DASHBOARD_TABLES),
        "sources": inactive,
    }


def clear_postgres_state_keep_sources() -> dict[str, Any]:
    return {
        "status": "ok",
        "dashboard": clear_dashboard_state_keep_sources(),
        "airflow": clear_airflow_history(),
        "superset": clear_superset_assets(),
        "hive_metastore": clear_hive_metastore(),
    }


def local_generated_artifact_count_keep_sources() -> dict[str, Any]:
    total_files = 0
    total_bytes = 0
    details: list[dict[str, Any]] = []
    for path in LOCAL_GENERATED_ARTIFACT_PATHS:
        if not path.exists():
            details.append({"path": str(path), "file_count": 0, "bytes": 0, "status": "missing"})
            continue
        file_count = 0
        byte_count = 0
        for item in path.rglob("*"):
            if path == STATE_DIR and item.name == "secrets.json":
                continue
            if item.is_file():
                file_count += 1
                try:
                    byte_count += item.stat().st_size
                except OSError:
                    pass
        total_files += file_count
        total_bytes += byte_count
        details.append({"path": str(path), "file_count": file_count, "bytes": byte_count, "status": "ok"})
    return {"file_count": total_files, "bytes": total_bytes, "paths": details}


def clear_object_storage_keep_sources() -> dict[str, Any]:
    if not docker_running(platform_container("minio")):
        cleared = remove_path_contents(DATA_DIR / "storage")
        return {
            "status": "ok",
            "method": "local_storage_path",
            "message": "MinIO container is not running; cleared local generated storage path.",
            "result": cleared,
        }
    try:
        from full_platform_wipe_reset import clear_minio_layers

        result = clear_minio_layers()
        return {"status": "ok", "method": "minio_api", "result": result}
    except Exception as exc:
        cleared = remove_path_contents(DATA_DIR / "storage")
        return {
            "status": "warning",
            "method": "local_storage_path",
            "message": f"MinIO API was unavailable; cleared local generated storage path instead: {exc}",
            "result": cleared,
        }


def safe_count_bucket_objects_keep_sources(bucket_name: str | None, prefix: str = "") -> dict[str, Any]:
    if not bucket_name:
        return {"object_count": 0, "storage_bytes": 0, "status": "missing"}
    if not docker_running(platform_container("minio")):
        return {
            "object_count": 0,
            "storage_bytes": 0,
            "status": "missing",
            "message": "MinIO container is not running.",
        }
    try:
        return {**count_bucket_objects(bucket_name, prefix), "status": "ok"}
    except Exception as exc:
        return {"object_count": 0, "storage_bytes": 0, "status": "error", "message": str(exc)}


def clear_local_artifacts_keep_source_secrets() -> dict[str, Any]:
    pycache_roots: list[Path] = []
    results = [
        remove_path_contents(path, preserve_names={"secrets.json"} if path == STATE_DIR else None)
        for path in LOCAL_GENERATED_ARTIFACT_PATHS
    ]
    results.extend(remove_path_contents(path) for path in pycache_roots if path.exists())
    return {"status": "ok", "paths": results, "preserved_state_files": ["secrets.json"]}


def post_wipe_residual_summary(estimate: dict[str, Any]) -> dict[str, Any]:
    local_files = int((estimate.get("local_artifacts") or {}).get("file_count") or 0)
    residuals = {
        field: int(estimate.get(field) or 0)
        for field in POST_WIPE_ZERO_ESTIMATE_FIELDS
        if int(estimate.get(field) or 0) != 0
    }
    if local_files:
        residuals["local_files"] = local_files
    return residuals


def clear_post_wipe_residuals_keep_sources() -> dict[str, Any]:
    return {
        "status": "ok",
        "dashboard": clear_dashboard_state_keep_sources(),
        "local_artifacts": clear_local_artifacts_keep_source_secrets(),
    }


def start_required_services_with_pipelines_paused() -> dict[str, Any]:
    started = [start_container(name) for name in RESET_START_ORDER]
    paused = pause_airflow_dags_with_retry(True)
    return {"started_containers": started, "paused_dags": paused}


def pause_airflow_dags_in_db(paused: bool) -> list[dict[str, Any]]:
    try:
        with psycopg2.connect(**postgres_config(os.environ["AIRFLOW_DB_NAME"])) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE dag SET is_paused = %s WHERE dag_id = ANY(%s) RETURNING dag_id, is_paused",
                    (paused, AIRFLOW_DAGS),
                )
                return [{"dag_id": row[0], "status": "ok", "is_paused": bool(row[1]), "method": "airflow_db"} for row in cursor.fetchall()]
    except Exception as exc:
        return [{"dag_id": dag_id, "status": "error", "message": str(exc), "method": "airflow_db"} for dag_id in AIRFLOW_DAGS]


def pause_airflow_dags_with_retry(paused: bool, attempts: int = 12, delay_seconds: float = 5.0) -> list[dict[str, Any]]:
    last_result: list[dict[str, Any]] = []
    for _ in range(attempts):
        last_result = pause_airflow_dags(paused)
        if last_result and all(item.get("status") == "ok" for item in last_result):
            return last_result
        time.sleep(delay_seconds)
    fallback = pause_airflow_dags_in_db(paused)
    return [
        {
            "status": "warning",
            "message": "Airflow API pause did not complete before timeout; applied database pause fallback.",
            "api_result": last_result,
            "db_result": fallback,
        }
    ]


def estimate_full_platform_wipe_keep_sources() -> dict[str, Any]:
    load_environment()
    existing = existing_dashboard_tables()
    clearable_tables = clearable_dashboard_tables(existing)
    counts = dashboard_table_counts(clearable_tables)
    clearable_set = set(clearable_tables)
    estimate_boot_excluded = ESTIMATE_EXCLUDED_BOOT_TABLES & clearable_set
    metadata_counts = {
        table_name: counts.get(table_name, 0)
        for table_name in METADATA_ROW_ESTIMATE_TABLES
        if table_name in clearable_set and table_name not in estimate_boot_excluded
    }

    def count_group(table_names: list[str], exclude: set[str] | None = None) -> int:
        excluded = (exclude or set()) | estimate_boot_excluded
        return sum(counts.get(table_name, 0) for table_name in (set(table_names) & clearable_set) - excluded)

    validation_tables = set(VALIDATION_RECONCILIATION_TABLES)
    category_counts = {
        "raw": count_group(RAW_TABLES),
        "bronze": count_group([*BRONZE_TABLES, "bronze_file_attempts"]),
        "silver": count_group(SILVER_TABLES),
        "query": count_group(QUERY_TABLES, validation_tables),
        "bi": count_group(BI_TABLES, validation_tables),
        "governance": count_group(GOVERNANCE_TABLES),
        "logs": count_group(LOG_TABLES),
        "monitoring": count_group(MONITORING_TABLES),
        "operations_history": count_group(OPERATIONS_HISTORY_TABLES),
        "replay_queue": count_group(REPLAY_QUEUE_TABLES),
        "service_health_history": count_group(SERVICE_HEALTH_HISTORY_TABLES),
        "service_restart_history": count_group(SERVICE_RESTART_HISTORY_TABLES),
        "validation_reconciliation": count_group(VALIDATION_RECONCILIATION_TABLES),
        "onboarding_cache_runtime": count_group(ONBOARDING_CACHE_RUNTIME_TABLES),
    }
    known_grouped_tables = set().union(*DASHBOARD_TABLE_GROUPS.values())
    unclassified_counts = {
        table_name: counts.get(table_name, 0)
        for table_name in clearable_tables
        if table_name not in known_grouped_tables and table_name not in estimate_boot_excluded
    }
    raw_bucket = safe_count_bucket_objects_keep_sources(os.environ.get("MINIO_BUCKET_RAW"))
    delta_bucket = os.environ.get("MINIO_BUCKET_DELTA")
    bronze_bucket = safe_count_bucket_objects_keep_sources(delta_bucket, "bronze/")
    silver_bucket = safe_count_bucket_objects_keep_sources(delta_bucket, "silver/")
    audit_bucket = safe_count_bucket_objects_keep_sources(os.environ.get("MINIO_BUCKET_AUDIT"))
    source_count = dashboard_table_count("source_connections")
    source_collection_count = dashboard_table_count("source_connection_collections")
    active_count = 0
    active_collection_count = 0
    if source_count:
        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM source_connections WHERE is_active = true")
                active_count = int(cursor.fetchone()[0])
                cursor.execute("SELECT count(*) FROM source_connection_collections WHERE is_active = true")
                active_collection_count = int(cursor.fetchone()[0])
    local_artifacts = local_generated_artifact_count_keep_sources()
    return {
        "status": "ok",
        "source_connections_preserved": source_count,
        "source_connection_collections_preserved": source_collection_count,
        "source_connections_to_deactivate": active_count,
        "source_collection_selections_preserved": active_collection_count,
        "metadata_rows": sum(metadata_counts.values()),
        "metadata_table_counts": metadata_counts,
        "metadata_category_counts": category_counts,
        "unclassified_clearable_table_counts": unclassified_counts,
        "clearable_tables": clearable_tables,
        "estimate_excluded_boot_tables": sorted(estimate_boot_excluded),
        "preserved_tables": sorted(existing & PRESERVED_DASHBOARD_TABLES),
        "raw_files": max(counts.get("raw_files", 0), int(raw_bucket.get("object_count", 0))),
        "bronze_tables": counts.get("bronze_collection_states", 0),
        "silver_tables": counts.get("silver_collection_states", 0),
        "dashboards": counts.get("bi_dashboards", 0),
        "raw_metadata_rows": category_counts.get("raw", 0),
        "bronze_metadata_rows": category_counts.get("bronze", 0),
        "silver_metadata_rows": category_counts.get("silver", 0),
        "query_metadata_rows": category_counts.get("query", 0),
        "bi_metadata_rows": category_counts.get("bi", 0),
        "governance_metadata_rows": category_counts.get("governance", 0),
        "log_rows": category_counts.get("logs", 0),
        "monitoring_rows": category_counts.get("monitoring", 0),
        "operations_history_rows": category_counts.get("operations_history", 0),
        "replay_queue_count": category_counts.get("replay_queue", 0),
        "service_health_history_rows": category_counts.get("service_health_history", 0),
        "service_restart_history_rows": category_counts.get("service_restart_history", 0),
        "validation_reconciliation_rows": category_counts.get("validation_reconciliation", 0),
        "onboarding_cache_runtime_rows": category_counts.get("onboarding_cache_runtime", 0),
        "runtime_rows": category_counts.get("onboarding_cache_runtime", 0),
        "local_artifacts": local_artifacts,
        "object_storage": {
            "raw": raw_bucket,
            "bronze": bronze_bucket,
            "silver": silver_bucket,
            "audit": audit_bucket,
        },
    }


def run_full_platform_wipe_keep_sources(triggered_by: str = "dashboard") -> dict[str, Any]:
    load_environment()
    started = time.monotonic()
    before_sources = source_connection_signatures()
    before_collections = source_collection_signatures()
    if not before_sources:
        raise RuntimeError("Full wipe keep-sources requires at least one existing source connection")
    before_estimate = estimate_full_platform_wipe_keep_sources()
    steps: list[dict[str, Any]] = []

    steps.append({"step": "pause_and_stop_pipeline_services", "result": stop_pipelines_safely()})
    steps.append({"step": "clear_trino_bronze_silver_metadata", "result": clear_trino_metadata()})
    steps.append({"step": "stop_reset_services", "result": stop_reset_services()})
    steps.append({"step": "clear_raw_bronze_silver_object_storage", "result": clear_object_storage_keep_sources()})
    steps.append({"step": "clear_openmetadata_runtime_data", "result": clear_openmetadata_data()})
    steps.append({"step": "clear_databases_keep_source_connections", "result": clear_postgres_state_keep_sources()})
    steps.append({"step": "clear_generated_artifacts_keep_source_secrets", "result": clear_local_artifacts_keep_source_secrets()})
    steps.append({"step": "restart_services_with_pipelines_paused", "result": start_required_services_with_pipelines_paused()})
    steps.append({"step": "finalize_sources_inactive", "result": clear_dashboard_state_keep_sources()})

    after_sources = source_connection_signatures()
    after_collections = source_collection_signatures()
    assert_source_connections_preserved(before_sources, after_sources)
    assert_source_collections_preserved(before_collections, after_collections)
    after_estimate = estimate_full_platform_wipe_keep_sources()
    residuals = post_wipe_residual_summary(after_estimate)
    if residuals:
        steps.append(
            {
                "step": "clear_post_wipe_residual_activity",
                "residuals_before": residuals,
                "result": clear_post_wipe_residuals_keep_sources(),
            }
        )
        after_sources = source_connection_signatures()
        after_collections = source_collection_signatures()
        assert_source_connections_preserved(before_sources, after_sources)
        assert_source_collections_preserved(before_collections, after_collections)
        after_estimate = estimate_full_platform_wipe_keep_sources()
        remaining_residuals = post_wipe_residual_summary(after_estimate)
        if remaining_residuals:
            raise RuntimeError(f"Post-wipe residual state remains after cleanup: {remaining_residuals}")
    return {
        "status": "ok",
        "message": "Full platform wipe completed; Source Connections were preserved and set inactive. No source ingestion was triggered.",
        "triggered_by": triggered_by,
        "confirmation": FULL_WIPE_KEEP_SOURCES_CONFIRMATION,
        "duration_seconds": round(time.monotonic() - started, 2),
        "estimated_deleted": before_estimate,
        "post_wipe_estimate": after_estimate,
        "source_connections_before": len(before_sources),
        "source_connections_after": len(after_sources),
        "source_collections_before": len(before_collections),
        "source_collections_after": len(after_collections),
        "sources_inactive": True,
        "onboarding": {
            "storage_key": ONBOARDING_STORAGE_KEY,
            "reset_to_step": "welcome",
            "client_action": "remove_local_storage_and_open_setup_wizard",
        },
        "steps": steps,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Full platform wipe while preserving and deactivating source connections.")
    parser.add_argument("--confirmation", required=False, default="")
    parser.add_argument("--triggered-by", default="cli")
    parser.add_argument("--estimate", action="store_true")
    args = parser.parse_args()
    if args.estimate:
        print(json.dumps(estimate_full_platform_wipe_keep_sources(), indent=2, default=str, sort_keys=True))
        return 0
    if args.confirmation != FULL_WIPE_KEEP_SOURCES_CONFIRMATION:
        raise SystemExit(f'Confirmation required: --confirmation "{FULL_WIPE_KEEP_SOURCES_CONFIRMATION}"')
    result = run_full_platform_wipe_keep_sources(triggered_by=args.triggered_by)
    print(json.dumps(result, indent=2, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
