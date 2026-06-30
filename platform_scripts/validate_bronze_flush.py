from __future__ import annotations

import os
import time
from typing import Any

import requests

from common import s3_client, setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db


TERMINAL_BRONZE = {"success", "no_new_data", "failed"}


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 60), **kwargs)
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


def bronze_object_count() -> int:
    return object_count(os.environ["MINIO_BUCKET_DELTA"], "bronze/")


def raw_object_count() -> int:
    return object_count(os.environ["MINIO_BUCKET_RAW"], "python/")


def wait_for_bronze(airflow_run_id: str, logger) -> dict[str, Any]:
    deadline = time.time() + int(os.environ.get("BRONZE_RUN_TIMEOUT_SECONDS", "420"))
    latest: dict[str, Any] = {"status": "queued", "airflow_run_id": airflow_run_id}
    while time.time() < deadline:
        runs = request_json("GET", "/api/pipelines/bronze/runs", timeout=30)
        matches = [run for run in runs if run.get("airflow_run_id") == airflow_run_id]
        if matches:
            latest = matches[0]
            if latest.get("status") in TERMINAL_BRONZE:
                break
        time.sleep(5)
    logger.info("Bronze run %s final status=%s", airflow_run_id, latest.get("status"))
    assert_ok(latest.get("status") in {"success", "no_new_data"}, f"Bronze run failed or timed out: {latest}")
    return latest


def run_bronze(logger) -> dict[str, Any]:
    payload = request_json("POST", "/api/pipelines/bronze/run", timeout=30)
    return wait_for_bronze(payload["run"]["airflow_run_id"], logger)


def metadata_counts() -> dict[str, int]:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            counts = {}
            for table in ("bronze_file_states", "bronze_processing_runs", "bronze_collection_states", "bronze_schema_snapshots", "bronze_column_mappings"):
                cursor.execute(f"SELECT count(*) FROM {table}")
                counts[table] = int(cursor.fetchone()[0])
            return counts


def main() -> None:
    logger = setup_logging("validate_bronze_flush")
    init_dashboard_db()

    run_bronze(logger)
    assert_ok(bronze_object_count() > 0, "Expected Bronze objects after processing")
    raw_before = raw_object_count()
    assert_ok(raw_before > 0, "Expected Raw files before Bronze flush")

    request_json("POST", "/api/bronze/flush", json={"confirmation": "FLUSH BRONZE ONLY"}, timeout=120)
    assert_ok(bronze_object_count() == 0, "Bronze objects remain after flush")
    counts_after_flush = metadata_counts()
    assert_ok(all(value == 0 for value in counts_after_flush.values()), f"Bronze metadata tables not empty after flush: {counts_after_flush}")
    assert_ok(raw_object_count() == raw_before, "Raw files changed during Bronze flush")

    rebuild = request_json("POST", "/api/bronze/rebuild", json={"confirmation": "REBUILD BRONZE"}, timeout=120)
    wait_for_bronze(rebuild["run"]["airflow_run_id"], logger)
    assert_ok(bronze_object_count() > 0, "Bronze objects did not return after rebuild")
    counts_after_rebuild = metadata_counts()
    assert_ok(counts_after_rebuild["bronze_file_states"] > 0, "Bronze file states did not return after rebuild")
    assert_ok(counts_after_rebuild["bronze_collection_states"] > 0, "Bronze collection states did not return after rebuild")
    assert_ok(counts_after_rebuild["bronze_schema_snapshots"] > 0, "Bronze schema snapshots did not return after rebuild")
    assert_ok(raw_object_count() == raw_before, "Raw files changed during Bronze rebuild")

    logger.info("Bronze flush and rebuild validation passed")


if __name__ == "__main__":
    main()
