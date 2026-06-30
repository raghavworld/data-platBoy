from __future__ import annotations

import argparse
import os
import time
from typing import Any

import pymongo
import requests

from common import s3_client, setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db
from source_secrets import get_secret


REQUIRED_TABLES = {
    "source_connections",
    "raw_ingestion_runs",
    "raw_collection_states",
    "raw_schema_snapshots",
    "raw_files",
    "raw_batch_fingerprints",
    "raw_collection_run_statuses",
    "raw_maintenance_events",
    "service_health_checks",
}


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def airflow_base() -> str:
    return os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080")


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def request_json(method: str, url: str, **kwargs: Any) -> Any:
    response = requests.request(method, url, timeout=kwargs.pop("timeout", 20), **kwargs)
    response.raise_for_status()
    return response.json()


def source_uri(source: dict[str, Any]) -> str:
    secret = get_secret(source.get("secret_reference"))
    if secret.get("mongo_uri"):
        return secret["mongo_uri"]
    config = source.get("connection_config_json") or {}
    host = config.get("host")
    port = config.get("port", 27017)
    auth_database = source.get("auth_database") or "admin"
    return f"mongodb://{host}:{port}/{auth_database}"


def table_counts() -> dict[str, int]:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            counts = {}
            for table in REQUIRED_TABLES:
                cursor.execute(f"SELECT count(*) FROM {table}")
                counts[table] = int(cursor.fetchone()[0])
            return counts


def fetch_sources() -> list[dict[str, Any]]:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id::text, source_name, database_name, auth_database,
                       connection_config_json, secret_reference,
                       include_collections_json, exclude_collections_json, is_active
                FROM source_connections
                ORDER BY source_name
                """
            )
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def ensure_raw_ingestion(logger, should_trigger: bool) -> None:
    counts = table_counts()
    if counts["raw_files"] > 0 and not should_trigger:
        logger.info("Raw files already cataloged; not triggering a new run")
        return

    payload = request_json("POST", f"{api_base()}/api/pipelines/raw/run", timeout=30)
    airflow_run_id = payload["run"]["airflow_run_id"]
    logger.info("Triggered raw DAG run %s", airflow_run_id)

    deadline = time.time() + int(os.environ.get("VALIDATE_RAW_TIMEOUT_SECONDS", "180"))
    terminal = {"success", "no_new_data", "duplicate_batch_skipped", "failed"}
    last_status = "queued"
    while time.time() < deadline:
        runs = request_json("GET", f"{api_base()}/api/pipelines/runs", timeout=20)
        matched = [run for run in runs if run.get("airflow_run_id") == airflow_run_id]
        if matched:
            last_status = matched[0].get("status")
            if last_status in terminal:
                break
        time.sleep(5)

    assert_ok(last_status in {"success", "no_new_data", "duplicate_batch_skipped"}, f"Raw DAG run did not succeed; final status={last_status}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trigger", action="store_true", help="Force a new raw ingestion run")
    args = parser.parse_args()

    logger = setup_logging("validate_raw_phase")
    logger.info("Validating Phase 1 Raw foundation")

    health = request_json("GET", f"{api_base()}/api/health")
    assert_ok(health.get("status") == "ok", "Dashboard API is not healthy")

    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """
            )
            tables = {row[0] for row in cursor.fetchall()}
    missing = REQUIRED_TABLES - tables
    assert_ok(not missing, f"Dashboard Postgres tables missing: {sorted(missing)}")

    request_json("POST", f"{api_base()}/api/sources/seed-demo")
    sources = fetch_sources()
    active_sources = [source for source in sources if source["is_active"]]
    assert_ok(len(sources) >= 4, "Expected at least four demo source connections")
    assert_ok(len(active_sources) >= 4, "Expected at least four active demo source connections")

    for source in active_sources:
        with pymongo.MongoClient(source_uri(source), serverSelectionTimeoutMS=5000, tz_aware=True) as client:
            client.admin.command("ping")
            include = source.get("include_collections_json") or []
            for collection_name in include:
                count = client[source["database_name"]][collection_name].count_documents({})
                assert_ok(count > 0, f"{source['database_name']}.{collection_name} has no documents")
        logger.info("Mongo source reachable: %s", source["source_name"])

    bucket_name = os.environ["MINIO_BUCKET_RAW"]
    buckets = {bucket["Name"] for bucket in s3_client().list_buckets().get("Buckets", [])}
    assert_ok(bucket_name in buckets, f"MinIO raw bucket missing: {bucket_name}")

    dag = request_json(
        "GET",
        f"{airflow_base()}/api/v1/dags/raw_ingestion_pipeline",
        auth=(os.environ.get("AIRFLOW_USER", "admin"), os.environ.get("AIRFLOW_PASSWORD", "admin")),
    )
    assert_ok(dag.get("dag_id") == "raw_ingestion_pipeline", "Airflow raw_ingestion_pipeline DAG missing")

    ensure_raw_ingestion(logger, args.trigger)

    objects = s3_client().list_objects_v2(Bucket=bucket_name, Prefix="python/")
    assert_ok(objects.get("KeyCount", 0) > 0, "No raw files found in MinIO")

    counts = table_counts()
    assert_ok(counts["raw_files"] > 0, "raw_files table has no entries")
    assert_ok(counts["raw_collection_states"] > 0, "raw_collection_states table has no entries")
    assert_ok(counts["raw_schema_snapshots"] > 0, "raw_schema_snapshots table has no entries")

    overview = request_json("GET", f"{api_base()}/api/raw/overview")
    assert_ok(overview.get("active_sources", 0) >= 4, "Raw overview active source count is invalid")
    files = request_json("GET", f"{api_base()}/api/raw/files")
    assert_ok(len(files) > 0, "Raw files endpoint returned no files")
    preview = request_json("GET", f"{api_base()}/api/raw/files/preview", params={"object_key": files[0]["object_key"]})
    assert_ok(len(preview.get("records", [])) > 0, "Raw preview endpoint returned no records")
    schema_changes = request_json("GET", f"{api_base()}/api/raw/schema-changes")
    cursor_state = request_json("GET", f"{api_base()}/api/raw/cursor-state")
    assert_ok(len(schema_changes) > 0, "Schema changes endpoint returned no snapshots")
    assert_ok(len(cursor_state) > 0, "Cursor state endpoint returned no states")

    logger.info("Phase 1 Raw validation passed")


if __name__ == "__main__":
    main()
