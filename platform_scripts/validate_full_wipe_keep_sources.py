#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from typing import Any

import psycopg2
import requests
from psycopg2 import sql
from psycopg2.extras import DictCursor

from common import PROJECT_ROOT, load_environment, s3_client, trino_connection
from dashboard_db import dashboard_connection, init_dashboard_db
from full_platform_wipe_keep_sources import (
    FULL_WIPE_EMPTY_START_SETTING,
    FULL_WIPE_KEEP_SOURCES_CONFIRMATION,
    estimate_full_platform_wipe_keep_sources,
)
from full_platform_wipe_reset import (
    AIRFLOW_DAGS,
    BI_TABLES,
    BRONZE_TABLES,
    GOVERNANCE_TABLES,
    OPERATIONS_TABLES,
    QUERY_TABLES,
    RAW_TABLES,
    SILVER_TABLES,
    count_bucket_objects,
    postgres_config,
    postgres_tables,
)


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def dashboard_count(table_name: str) -> int:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = %s
                """,
                (table_name,),
            )
            if not cursor.fetchone():
                return 0
            cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table_name)))
            return int(cursor.fetchone()[0])


def dashboard_counts(table_names: list[str]) -> dict[str, int]:
    return {table_name: dashboard_count(table_name) for table_name in table_names}


def validate_sources_preserved_inactive() -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT source_name, database_name, source_type, is_active, secret_reference
                FROM source_connections
                ORDER BY source_name
                """
            )
            rows = [dict(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT count(*) AS total,
                       count(*) FILTER (WHERE is_active) AS active,
                       COALESCE(sum(record_count), 0) AS record_count,
                       COALESCE(sum(estimated_size_bytes), 0) AS estimated_size_bytes,
                       count(*) FILTER (
                           WHERE avg_object_size IS NOT NULL
                              OR last_stats_at IS NOT NULL
                              OR detected_cursor_field IS NOT NULL
                              OR detected_cursor_strategy IS NOT NULL
                              OR sample_schema_json <> '{}'::jsonb
                              OR last_error IS NOT NULL
                              OR last_scheduled_run_at IS NOT NULL
                              OR next_scheduled_run_at IS NOT NULL
                       ) AS stale_cache
                FROM source_connection_collections
                """
            )
            collection_row = dict(cursor.fetchone())
    assert_ok(rows, "source_connections is empty")
    active = [row["source_name"] for row in rows if row.get("is_active")]
    assert_ok(not active, f"Source Connections are still active: {active}")
    active_collections = int(collection_row.get("active") or 0)
    assert_ok(active_collections == 0, f"Source Connection collections are still active: {active_collections}")
    assert_ok(int(collection_row.get("record_count") or 0) == 0, "Source collection inventory record counts were not reset")
    assert_ok(int(collection_row.get("estimated_size_bytes") or 0) == 0, "Source collection inventory sizes were not reset")
    assert_ok(int(collection_row.get("stale_cache") or 0) == 0, "Source collection inventory/cache fields were not reset")
    return {
        "source_connections": len(rows),
        "inactive": len(rows),
        "source_collections_preserved": int(collection_row.get("total") or 0),
        "source_collections_inactive": int(collection_row.get("total") or 0),
        "sources": [
            {
                "source_name": row["source_name"],
                "database_name": row["database_name"],
                "source_type": row["source_type"],
                "secret_reference_preserved": bool(row.get("secret_reference")),
            }
            for row in rows
        ],
    }


def bucket_empty(bucket_name: str, prefix: str = "") -> dict[str, int]:
    stats = count_bucket_objects(bucket_name, prefix)
    assert_ok(stats["object_count"] == 0, f"Bucket {bucket_name}/{prefix} is not empty: {stats}")
    return stats


def validate_object_storage_empty() -> dict[str, Any]:
    client = s3_client()
    buckets = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    raw_bucket = os.environ["MINIO_BUCKET_RAW"]
    delta_bucket = os.environ["MINIO_BUCKET_DELTA"]
    assert_ok(raw_bucket in buckets, f"RAW bucket is missing: {raw_bucket}")
    assert_ok(delta_bucket in buckets, f"Delta bucket is missing: {delta_bucket}")
    return {
        "raw": bucket_empty(raw_bucket),
        "bronze": bucket_empty(delta_bucket, "bronze/"),
        "silver": bucket_empty(delta_bucket, "silver/"),
        "delta_all": bucket_empty(delta_bucket),
    }


def assert_tables_empty(table_names: list[str], label: str) -> dict[str, int]:
    counts = dashboard_counts(table_names)
    dirty = {table_name: count for table_name, count in counts.items() if count}
    assert_ok(not dirty, f"{label} tables are not empty: {dirty}")
    return counts


def validate_dashboard_state_cleared() -> dict[str, Any]:
    raw = assert_tables_empty(RAW_TABLES, "RAW")
    bronze = assert_tables_empty([*BRONZE_TABLES, "bronze_file_attempts"], "Bronze")
    silver = assert_tables_empty(SILVER_TABLES, "Silver")
    query = assert_tables_empty(QUERY_TABLES, "Query")
    bi = assert_tables_empty(BI_TABLES, "BI")
    governance = assert_tables_empty(GOVERNANCE_TABLES, "Governance")
    runtime = assert_tables_empty(
        [
            *OPERATIONS_TABLES,
            "raw_maintenance_events",
            "bronze_maintenance_events",
            "silver_maintenance_events",
            "backup_exports",
        ],
        "Runtime/history",
    )
    audit = validate_audit_logs_cleared()
    current_runtime = validate_current_runtime_signals_only()
    assert_ok(dashboard_count("platform_users") >= 1, "platform_users must remain available for console access")
    return {
        "raw": raw,
        "bronze": bronze,
        "silver": silver,
        "query": query,
        "bi": bi,
        "governance": governance,
        "runtime": runtime,
        "audit_logs": audit,
        "current_runtime_signals": current_runtime,
        "platform_users": dashboard_count("platform_users"),
    }


def validate_raw_overview_reset() -> dict[str, Any]:
    base_url = os.environ.get("DASHBOARD_API_URL", "http://dashboard-api:8001").rstrip("/")
    status_response = requests.get(f"{base_url}/api/raw/source-status?database_status=all", timeout=20)
    assert_ok(status_response.ok, f"RAW source status failed: {status_response.status_code} {status_response.text[:200]}")
    freshness_response = requests.get(f"{base_url}/api/raw/freshness", timeout=20)
    assert_ok(freshness_response.ok, f"RAW freshness failed: {freshness_response.status_code} {freshness_response.text[:200]}")

    payload = status_response.json()
    summary = payload.get("summary") or {}
    latest_totals = summary.get("latest_run_totals") or {}
    zero_summary_fields = [
        "active_databases",
        "active_collections",
        "selected_records",
        "selected_size_bytes",
        "raw_files",
        "rows_ingested",
        "processed_collections",
        "processed_collections_total",
        "no_new_data_collections",
        "no_new_data_collections_total",
        "failed_collections",
        "failed_collections_total",
        "fresh_collections",
        "delayed_collections",
        "stale_collections",
        "never_ingested_collections",
        "latest_run_raw_files",
        "latest_run_rows_ingested",
    ]
    dirty_summary = {field: summary.get(field) for field in zero_summary_fields if int(summary.get(field) or 0) != 0}
    assert_ok(not dirty_summary, f"RAW overview counters did not reset: {dirty_summary}")
    zero_latest_fields = [
        "target_collections",
        "processed_collections",
        "no_new_data_collections",
        "failed_collections",
        "raw_files",
        "rows_ingested",
        "records_processed",
        "estimated_records",
        "total_batches",
        "completed_batches",
        "queued_batches",
        "running_batches",
        "failed_batches",
        "skipped_batches",
    ]
    dirty_latest = {field: latest_totals.get(field) for field in zero_latest_fields if int(latest_totals.get(field) or 0) != 0}
    assert_ok(not dirty_latest, f"RAW latest-run counters did not reset: {dirty_latest}")

    freshness = freshness_response.json().get("summary") or {}
    active_freshness_fields = ["fresh", "delayed", "stale", "never_ingested"]
    dirty_freshness = {field: freshness.get(field) for field in active_freshness_fields if int(freshness.get(field) or 0) != 0}
    assert_ok(not dirty_freshness, f"Active freshness counters did not reset: {dirty_freshness}")
    return {
        "summary_zero_fields": zero_summary_fields,
        "latest_run_zero_fields": zero_latest_fields,
        "inactive_freshness_collections": int(freshness.get("inactive") or 0),
        "sources_loaded": len(payload.get("sources") or []),
    }


def validate_metadata_rows_minimal() -> dict[str, Any]:
    estimate = estimate_full_platform_wipe_keep_sources()
    metadata_rows = int(estimate.get("metadata_rows") or 0)
    assert_ok(metadata_rows == 0, f"Post-wipe clearable metadata rows are not minimal: {metadata_rows}")
    return {
        "metadata_rows": metadata_rows,
        "metadata_table_counts": estimate.get("metadata_table_counts") or {},
        "source_connections_preserved": estimate.get("source_connections_preserved"),
    }


def validate_audit_logs_cleared() -> dict[str, Any]:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT actor, role, action, status, created_at FROM audit_logs ORDER BY created_at DESC")
            rows = [dict(row) for row in cursor.fetchall()]
    assert_ok(not rows, f"Audit logs remain after wipe: {rows[:5]}")
    return {
        "retained_audit_logs": 0,
        "current_console_activity": 0,
        "transient_stale_client_denials": 0,
    }


def validate_current_runtime_signals_only() -> dict[str, Any]:
    alerts = dashboard_count("platform_alerts")
    health_checks = dashboard_count("service_health_checks")
    sessions = dashboard_count("user_sessions")
    with dashboard_connection() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT severity, source, message, status FROM platform_alerts")
            alert_rows = [dict(row) for row in cursor.fetchall()]
    assert_ok(not alert_rows, f"Platform alerts remain after wipe: {alert_rows[:5]}")
    assert_ok(health_checks == 0, f"Service health history remains after wipe: {health_checks}")
    return {
        "platform_alerts_current": alerts,
        "service_health_checks_current": health_checks,
        "user_sessions_current": sessions,
    }


def validate_onboarding_readiness_reset() -> dict[str, Any]:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT setting_value_json
                FROM platform_settings
                WHERE setting_key = %s
                """,
                (FULL_WIPE_EMPTY_START_SETTING,),
            )
            row = cursor.fetchone()
    assert_ok(row is not None and row[0] is True, "onboarding.full_wipe_empty_start is not set to true")
    validation_counts = {
        "query_validation_runs": dashboard_count("query_validation_runs"),
        "bi_validation_runs": dashboard_count("bi_validation_runs"),
        "global_validation_runs": dashboard_count("global_validation_runs"),
        "silver_transformation_quality": dashboard_count("silver_transformation_quality"),
        "silver_quality_metrics": dashboard_count("silver_quality_metrics"),
    }
    assert_ok(not any(validation_counts.values()), f"Validation history is not cleared: {validation_counts}")
    return {"empty_start": True, "validation_counts": validation_counts}


def validate_airflow_history_and_paused() -> dict[str, Any]:
    result: dict[str, Any] = {"active_dag_runs": 0, "paused_dags": []}
    try:
        with psycopg2.connect(**postgres_config(os.environ["AIRFLOW_DB_NAME"])) as connection:
            existing = postgres_tables(connection)
            with connection.cursor() as cursor:
                if "dag_run" in existing:
                    cursor.execute("SELECT count(*) FROM dag_run WHERE state IN ('queued', 'running')")
                    result["active_dag_runs"] = int(cursor.fetchone()[0])
                if "dag" in existing:
                    cursor.execute("SELECT dag_id, is_paused FROM dag WHERE dag_id = ANY(%s)", (AIRFLOW_DAGS,))
                    result["paused_dags"] = [{"dag_id": row[0], "is_paused": bool(row[1])} for row in cursor.fetchall()]
    except Exception as exc:
        result["warning"] = str(exc)
        return result
    assert_ok(result["active_dag_runs"] == 0, f"Airflow still has active DAG runs: {result['active_dag_runs']}")
    unpaused = [row["dag_id"] for row in result["paused_dags"] if not row["is_paused"]]
    assert_ok(not unpaused, f"Airflow DAGs are not paused: {unpaused}")
    return result


def validate_trino_empty() -> dict[str, Any]:
    output: dict[str, Any] = {}
    for schema_name in ["bronze", "silver"]:
        connection = trino_connection(schema=schema_name)
        cursor = connection.cursor()
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS delta.{schema_name}")
            cursor.execute(f"SHOW TABLES FROM delta.{schema_name}")
            tables = [row[0] for row in cursor.fetchall()]
            try:
                cursor.execute(f"SHOW VIEWS FROM delta.{schema_name}")
                views = [row[0] for row in cursor.fetchall()]
            except Exception:
                views = []
        finally:
            cursor.close()
            connection.close()
        assert_ok(not tables and not views, f"Trino delta.{schema_name} is not empty: tables={tables}, views={views}")
        output[schema_name] = {"tables": tables, "views": views}
    return output


def validate_superset_empty() -> dict[str, Any]:
    try:
        with psycopg2.connect(**postgres_config(os.environ["SUPERSET_DB_NAME"])) as connection:
            existing = postgres_tables(connection)
            counts: dict[str, int] = {}
            with connection.cursor() as cursor:
                for table_name in ["dashboards", "slices", "tables", "dbs", "logs", "query"]:
                    if table_name not in existing:
                        counts[table_name] = 0
                        continue
                    cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table_name)))
                    counts[table_name] = int(cursor.fetchone()[0])
        dirty = {table_name: count for table_name, count in counts.items() if count}
        assert_ok(not dirty, f"Superset generated assets/history remain: {dirty}")
        return counts
    except Exception as exc:
        return {"warning": str(exc)}


def validate_platform_and_sources_page() -> dict[str, Any]:
    base_url = os.environ.get("DASHBOARD_API_URL", "http://dashboard-api:8001").rstrip("/")
    health = requests.get(f"{base_url}/api/health", timeout=15)
    assert_ok(health.ok, f"Dashboard API health failed: {health.status_code} {health.text[:200]}")
    sources = requests.get(f"{base_url}/api/sources", timeout=15)
    assert_ok(sources.ok, f"Source Connections API failed: {sources.status_code} {sources.text[:200]}")
    payload = sources.json()
    assert_ok(isinstance(payload, list) and payload, "Source Connections page data is empty")
    assert_ok(all(not item.get("is_active") for item in payload), "Source Connections API returned active sources")
    return {"health": health.json(), "sources_loaded": len(payload)}


def validate_static_wiring() -> dict[str, Any]:
    root = os.environ.get("PROJECT_ROOT") or str(PROJECT_ROOT)
    main_js = open(os.path.join(root, "dashboard/web/src/main.jsx"), encoding="utf-8").read()
    api_py = open(os.path.join(root, "dashboard/api/app/main.py"), encoding="utf-8").read()
    makefile_path = os.path.join(root, "Makefile")
    makefile = open(makefile_path, encoding="utf-8").read() if os.path.exists(makefile_path) else ""
    assert_ok("/api/platform/full-wipe-keep-sources" in api_py, "API endpoint is not wired")
    assert_ok("/api/platform/full-wipe-keep-sources" in main_js, "Operations Center button is not wired")
    assert_ok("/api/platform/full-wipe-reset" not in main_js, "Operations Center button still references the source-deleting wipe endpoint")
    assert_ok(FULL_WIPE_KEEP_SOURCES_CONFIRMATION in main_js, "Operations Center confirmation text is missing")
    assert_ok("RESET EVERYTHING" not in main_js, "Operations Center still asks for the old second confirmation")
    assert_ok("Source Connections To Delete" not in main_js, "Operations Center still shows source-deleting copy")
    assert_ok("Source Connections Preserved" in main_js, "Operations Center preserved-source estimate is missing")
    assert_ok("Source Connections To Deactivate" in main_js, "Operations Center source deactivation estimate is missing")
    assert_ok("Run Raw Ingestion" not in main_js, "Global Run Raw Ingestion button copy still appears in main UI")
    assert_ok("Run Bronze</TextButton>" not in main_js, "Global Run Bronze button still appears in main UI")
    assert_ok("Run Silver</TextButton>" not in main_js, "Global Run Silver button still appears in main UI")
    assert_ok("Run Bronze Processing" not in main_js, "Bronze page still exposes a duplicated pipeline run button")
    raw_start = main_js.index("function RawPage")
    raw_end = main_js.index("function BronzePage", raw_start)
    raw_page = main_js[raw_start:raw_end]
    assert_ok('["maintenance", "Maintenance"]' not in raw_page, "RAW Layer still exposes the Maintenance tab")
    assert_ok("raw-overview-groups" in raw_page, "RAW Overview grouped card layout is missing")
    assert_ok("Selection / Inventory" in raw_page, "RAW Overview selection group is missing")
    assert_ok("RAW Processing" in raw_page, "RAW Overview processing group is missing")
    assert_ok("Health / Freshness" in raw_page, "RAW Overview freshness group is missing")
    assert_ok(
        "Clears RAW, Bronze, Silver, BI, governance, inventory caches, logs, validations, onboarding, runtime state, and platform history while preserving Source Connections and setting them inactive."
        in main_js,
        "Operations Center keep-sources description is missing",
    )
    if makefile:
        assert_ok("full-wipe-keep-sources:" in makefile, "Makefile command is missing")
        assert_ok("full_platform_wipe_keep_sources.py" in makefile, "Makefile wipe command is not using keep-sources implementation")
    return {"confirmation": FULL_WIPE_KEEP_SOURCES_CONFIRMATION, "makefile_visible": bool(makefile)}


def main() -> int:
    load_environment()
    checks = {
        "sources": validate_sources_preserved_inactive(),
        "object_storage": validate_object_storage_empty(),
        "dashboard_state": validate_dashboard_state_cleared(),
        "minimal_metadata_rows": validate_metadata_rows_minimal(),
        "onboarding_readiness": validate_onboarding_readiness_reset(),
        "raw_overview_reset": validate_raw_overview_reset(),
        "airflow": validate_airflow_history_and_paused(),
        "trino": validate_trino_empty(),
        "superset": validate_superset_empty(),
        "platform": validate_platform_and_sources_page(),
        "static_wiring": validate_static_wiring(),
    }
    print(json.dumps({"status": "ok", "checks": checks}, indent=2, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2, sort_keys=True))
        raise SystemExit(1)
