#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql

from common import DATA_DIR, load_environment, s3_client, trino_connection
from dashboard_db import dashboard_connection, init_dashboard_db
from full_platform_wipe_reset import (
    BI_TABLES,
    BRONZE_TABLES,
    GOVERNANCE_TABLES,
    LOG_ESTIMATE_TABLES,
    ONBOARDING_STORAGE_KEY,
    QUERY_TABLES,
    RAW_TABLES,
    SILVER_TABLES,
    count_bucket_objects,
    mongo_source_counts,
    postgres_config,
    postgres_tables,
)


ROOT = Path(__file__).resolve().parents[1]


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


def assert_dashboard_tables_empty(table_names: list[str], label: str) -> dict[str, int]:
    counts = {table_name: dashboard_count(table_name) for table_name in table_names}
    dirty = {table_name: count for table_name, count in counts.items() if count}
    assert_ok(not dirty, f"{label} tables are not empty: {dirty}")
    return counts


def bucket_empty(bucket_name: str, prefix: str = "") -> dict[str, int]:
    stats = count_bucket_objects(bucket_name, prefix)
    assert_ok(stats["object_count"] == 0, f"Bucket {bucket_name}/{prefix} is not empty: {stats}")
    return stats


def trino_schema_empty(schema_name: str) -> dict[str, Any]:
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
    return {"tables": tables, "views": views}


def superset_asset_counts() -> dict[str, int]:
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
            return counts
    except Exception as exc:
        return {"_status": -1, "_message": str(exc)}


def validate_mongo_preserved() -> dict[str, Any]:
    counts = mongo_source_counts()
    failed = [item for item in counts["sources"] if item["status"] != "ok"]
    assert_ok(not failed, f"Mongo source checks failed: {failed}")
    assert_ok(counts["total_documents"] > 0, "Mongo source data is missing or empty")
    return counts


def validate_object_storage_empty() -> dict[str, Any]:
    client = s3_client()
    buckets = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    raw_bucket = os.environ["MINIO_BUCKET_RAW"]
    delta_bucket = os.environ["MINIO_BUCKET_DELTA"]
    audit_buckets = sorted({name for name in [os.environ.get("MINIO_BUCKET_AUDIT", ""), *buckets] if name and "audit" in name.lower()})
    assert_ok(raw_bucket in buckets, f"RAW bucket is missing: {raw_bucket}")
    assert_ok(delta_bucket in buckets, f"Delta bucket is missing: {delta_bucket}")
    return {
        "raw": bucket_empty(raw_bucket),
        "bronze": bucket_empty(delta_bucket, "bronze/"),
        "silver": bucket_empty(delta_bucket, "silver/"),
        "delta_all": bucket_empty(delta_bucket),
        "audit": [bucket_empty(bucket_name) for bucket_name in audit_buckets],
    }


def validate_dashboard_empty() -> dict[str, Any]:
    raw = assert_dashboard_tables_empty(RAW_TABLES, "RAW")
    bronze = assert_dashboard_tables_empty(BRONZE_TABLES, "Bronze")
    silver = assert_dashboard_tables_empty(SILVER_TABLES, "Silver")
    query = assert_dashboard_tables_empty(QUERY_TABLES, "Query")
    bi = assert_dashboard_tables_empty(BI_TABLES, "BI")
    governance = assert_dashboard_tables_empty(GOVERNANCE_TABLES, "Governance")
    logs = assert_dashboard_tables_empty(LOG_ESTIMATE_TABLES, "History/log")
    source_connections = dashboard_count("source_connections")
    assert_ok(source_connections > 0, "Source connections were not preserved")
    assert_ok(dashboard_count("platform_users") >= 1, "Default platform users were not restored")
    assert_ok(dashboard_count("platform_settings") >= 1, "Default platform settings were not restored")
    return {
        "raw": raw,
        "bronze": bronze,
        "silver": silver,
        "query": query,
        "bi": bi,
        "governance": governance,
        "logs": logs,
        "source_connections_preserved": source_connections,
    }


def validate_superset_empty() -> dict[str, Any]:
    dashboard_counts = {
        "bi_datasets": dashboard_count("bi_datasets"),
        "bi_dashboards": dashboard_count("bi_dashboards"),
        "bi_validation_runs": dashboard_count("bi_validation_runs"),
    }
    assert_ok(not any(dashboard_counts.values()), f"Dashboard BI metadata is not empty: {dashboard_counts}")
    superset_counts = superset_asset_counts()
    if "_status" not in superset_counts:
        dirty = {table_name: count for table_name, count in superset_counts.items() if count}
        assert_ok(not dirty, f"Superset assets are not empty: {dirty}")
    return {"dashboard": dashboard_counts, "superset": superset_counts}


def validate_onboarding_reset() -> dict[str, Any]:
    main_js = (ROOT / "dashboard" / "web" / "src" / "main.jsx").read_text(encoding="utf-8")
    assert_ok("/api/platform/full-wipe-reset" in main_js, "Full wipe UI endpoint is not wired")
    assert_ok("localStorage.removeItem(ONBOARDING_STORAGE_KEY)" in main_js, "Full wipe UI does not clear onboarding progress")
    source_connections = dashboard_count("source_connections")
    assert_ok(source_connections > 0, "Source connection records are not available after reset")
    return {"storage_key": ONBOARDING_STORAGE_KEY, "source_connections_preserved": source_connections}


def validate_platform_fresh() -> dict[str, Any]:
    validations = {
        "query_validation_runs": dashboard_count("query_validation_runs"),
        "bi_validation_runs": dashboard_count("bi_validation_runs"),
        "global_validation_runs": dashboard_count("global_validation_runs"),
        "silver_quality_metrics": dashboard_count("silver_quality_metrics"),
    }
    assert_ok(not any(validations.values()), f"Validation history is not cleared: {validations}")
    history = {
        "operations_history": dashboard_count("operations_history"),
        "platform_events": dashboard_count("platform_events"),
        "audit_logs": dashboard_count("audit_logs"),
        "replay_jobs": dashboard_count("replay_jobs"),
        "service_restart_history": dashboard_count("service_restart_history"),
        "platform_alerts": dashboard_count("platform_alerts"),
        "service_health_checks": dashboard_count("service_health_checks"),
    }
    assert_ok(not any(history.values()), f"Operational history is not cleared: {history}")
    return {"validations": validations, "history": history}


def main() -> int:
    load_environment()
    results = {
        "mongo": validate_mongo_preserved(),
        "object_storage": validate_object_storage_empty(),
        "dashboard_db": validate_dashboard_empty(),
        "trino": {
            "bronze": trino_schema_empty("bronze"),
            "silver": trino_schema_empty("silver"),
        },
        "superset": validate_superset_empty(),
        "onboarding": validate_onboarding_reset(),
        "fresh": validate_platform_fresh(),
    }
    print(json.dumps({"status": "ok", "checks": results}, indent=2, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, indent=2, sort_keys=True))
        raise SystemExit(1)
