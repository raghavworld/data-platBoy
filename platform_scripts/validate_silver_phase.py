from __future__ import annotations

import os
import time
from typing import Any

import requests

from common import s3_client, setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db


TERMINAL = {"success", "no_new_data", "warning", "unmapped_bronze_tables", "failed", "cancelled"}
REQUIRED_TABLES = {
    "silver_processing_runs",
    "silver_collection_states",
    "silver_batch_fingerprints",
    "silver_schema_snapshots",
}


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def airflow_base() -> str:
    return os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080")


def request_json(method: str, path_or_url: str, absolute: bool = False, **kwargs: Any) -> Any:
    url = path_or_url if absolute else f"{api_base()}{path_or_url}"
    response = requests.request(method, url, timeout=kwargs.pop("timeout", 60), **kwargs)
    response.raise_for_status()
    return response.json()


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def object_count(bucket_name: str, prefix: str) -> int:
    count = 0
    paginator = s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        count += len(page.get("Contents", []))
    return count


def raw_object_count() -> int:
    return object_count(os.environ["MINIO_BUCKET_RAW"], "python/")


def bronze_object_count() -> int:
    return object_count(os.environ["MINIO_BUCKET_DELTA"], "bronze/")


def silver_object_count() -> int:
    return object_count(os.environ["MINIO_BUCKET_DELTA"], "silver/")


def wait_for_run(path: str, airflow_run_id: str, logger, timeout_env: str = "SILVER_RUN_TIMEOUT_SECONDS") -> dict[str, Any]:
    deadline = time.time() + int(os.environ.get(timeout_env, "480"))
    latest: dict[str, Any] = {"status": "queued", "airflow_run_id": airflow_run_id}
    while time.time() < deadline:
        runs = request_json("GET", path, timeout=30)
        matches = [run for run in runs if run.get("airflow_run_id") == airflow_run_id]
        if matches:
            latest = matches[0]
            if latest.get("status") in TERMINAL:
                break
        time.sleep(5)
    logger.info("Run %s ended status=%s", airflow_run_id, latest.get("status"))
    assert_ok(latest.get("status") in {"success", "no_new_data", "warning", "unmapped_bronze_tables"}, f"Run failed or timed out: {latest}")
    return latest


def run_silver(logger) -> dict[str, Any]:
    payload = request_json("POST", "/api/pipelines/silver/run", timeout=30)
    return wait_for_run("/api/pipelines/silver/runs", payload["run"]["airflow_run_id"], logger)


def run_bronze_rebuild(logger) -> None:
    payload = request_json("POST", "/api/bronze/rebuild", json={"confirmation": "REBUILD BRONZE"}, timeout=120)
    wait_for_run("/api/pipelines/bronze/runs", payload["run"]["airflow_run_id"], logger, timeout_env="BRONZE_RUN_TIMEOUT_SECONDS")


def ensure_bronze_ready(logger) -> None:
    if raw_object_count() == 0:
        raw_payload = request_json("POST", "/api/pipelines/raw/run", timeout=30)
        wait_for_run("/api/pipelines/runs", raw_payload["run"]["airflow_run_id"], logger, timeout_env="RAW_RUN_TIMEOUT_SECONDS")
    bronze_tables = request_json("GET", "/api/bronze/tables", timeout=60)
    if len(bronze_tables) < 1 or bronze_object_count() == 0:
        logger.info("Bronze is incomplete; rebuilding Bronze before Silver validation")
        run_bronze_rebuild(logger)


def silver_row_counts() -> dict[str, int]:
    tables = request_json("GET", "/api/silver/tables", timeout=60)
    return {table["silver_table_name"]: int(table.get("row_count") or 0) for table in tables}


def metadata_counts() -> dict[str, int]:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            counts = {}
            for table in REQUIRED_TABLES:
                cursor.execute(f"SELECT count(*) FROM {table}")
                counts[table] = int(cursor.fetchone()[0])
            return counts


def main() -> None:
    logger = setup_logging("validate_silver_phase")
    logger.info("Validating Phase 3 Silver layer")
    init_dashboard_db()

    health = request_json("GET", "/api/health")
    assert_ok(health.get("status") == "ok", "Dashboard API is not healthy")

    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            existing_tables = {row[0] for row in cursor.fetchall()}
    missing = REQUIRED_TABLES - existing_tables
    assert_ok(not missing, f"Silver dashboard tables missing: {sorted(missing)}")

    dag = request_json(
        "GET",
        f"{airflow_base()}/api/v1/dags/silver_processing_pipeline",
        absolute=True,
        auth=(os.environ.get("AIRFLOW_USER", "admin"), os.environ.get("AIRFLOW_PASSWORD", "admin")),
    )
    assert_ok(dag.get("dag_id") == "silver_processing_pipeline", "Airflow silver_processing_pipeline DAG missing")

    raw_before = raw_object_count()
    ensure_bronze_ready(logger)
    bronze_before = bronze_object_count()
    assert_ok(raw_before > 0, "Raw files are missing")
    assert_ok(bronze_before > 0, "Bronze files are missing")

    first_run = run_silver(logger)
    assert_ok(first_run.get("status") in {"success", "no_new_data", "warning", "unmapped_bronze_tables"}, "Silver run did not complete")
    assert_ok(silver_object_count() > 0, "No Silver Delta objects found")

    tables = request_json("GET", "/api/silver/tables", timeout=60)
    names = {table["silver_table_name"] for table in tables}
    assert_ok(names, "No dynamic Silver tables were discovered")
    assert_ok(all(int(table.get("row_count") or 0) >= 0 for table in tables), "Silver row counts are invalid")

    preview_table = sorted(names)[0]
    preview = request_json("GET", "/api/silver/preview", params={"table_name": preview_table, "limit": 10}, timeout=60)
    assert_ok(preview.get("records"), f"Silver preview returned no {preview_table} rows")
    first_record = preview["records"][0]
    assert_ok(first_record, f"{preview_table} preview returned an empty record")

    schemas = request_json("GET", "/api/silver/schema-snapshots", timeout=60)
    assert_ok(schemas, "Silver schema snapshots are missing")
    quality = request_json("GET", "/api/silver/data-quality", timeout=60)
    assert_ok(quality, "Silver data quality metrics are missing")
    overview = request_json("GET", "/api/silver/overview", timeout=60)
    assert_ok(overview.get("silver_tables", 0) >= len(names), "Silver overview table count is invalid")

    before_counts = silver_row_counts()
    second_run = run_silver(logger)
    after_counts = silver_row_counts()
    assert_ok(before_counts == after_counts, f"Silver row counts changed on no-op rerun: before={before_counts}, after={after_counts}")
    assert_ok(int(second_run.get("total_rows_written") or 0) == 0, "Silver no-op rerun wrote rows")

    raw_count_before_flush = raw_object_count()
    bronze_count_before_flush = bronze_object_count()
    request_json("POST", "/api/silver/flush", json={"confirmation": "FLUSH SILVER"}, timeout=120)
    assert_ok(silver_object_count() == 0, "Silver objects remain after flush")
    flush_counts = metadata_counts()
    assert_ok(all(value == 0 for value in flush_counts.values()), f"Silver metadata not empty after flush: {flush_counts}")
    assert_ok(raw_object_count() == raw_count_before_flush, "Raw objects changed during Silver flush")
    assert_ok(bronze_object_count() == bronze_count_before_flush, "Bronze objects changed during Silver flush")

    rebuild = request_json("POST", "/api/silver/rebuild", json={"confirmation": "REBUILD SILVER"}, timeout=120)
    wait_for_run("/api/pipelines/silver/runs", rebuild["run"]["airflow_run_id"], logger)
    assert_ok(silver_object_count() > 0, "Silver objects did not return after rebuild")
    assert_ok(set(silver_row_counts()), "Silver tables did not return after rebuild")
    assert_ok(raw_object_count() == raw_count_before_flush, "Raw objects changed during Silver rebuild")
    assert_ok(bronze_object_count() == bronze_count_before_flush, "Bronze objects changed during Silver rebuild")

    logger.info("Phase 3 Silver validation passed")


if __name__ == "__main__":
    main()
