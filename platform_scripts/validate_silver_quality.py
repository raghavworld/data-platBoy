from __future__ import annotations

from datetime import datetime
import os
import time
from typing import Any

import requests

from common import setup_logging


TERMINAL = {"success", "no_new_data", "warning", "unmapped_bronze_tables", "failed"}
def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 60), **kwargs)
    response.raise_for_status()
    return response.json()


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def wait_for_silver(airflow_run_id: str, logger) -> dict[str, Any]:
    deadline = time.time() + int(os.environ.get("SILVER_RUN_TIMEOUT_SECONDS", "480"))
    latest: dict[str, Any] = {"status": "queued", "airflow_run_id": airflow_run_id}
    while time.time() < deadline:
        runs = request_json("GET", "/api/pipelines/silver/runs", timeout=30)
        matches = [run for run in runs if run.get("airflow_run_id") == airflow_run_id]
        if matches:
            latest = matches[0]
            if latest.get("status") in TERMINAL:
                break
        time.sleep(5)
    logger.info("Silver run %s final status=%s", airflow_run_id, latest.get("status"))
    assert_ok(latest.get("status") in {"success", "no_new_data", "warning", "unmapped_bronze_tables"}, f"Silver run failed or timed out: {latest}")
    return latest


def ensure_silver(logger) -> None:
    tables = request_json("GET", "/api/silver/tables", timeout=60)
    if tables:
        return
    payload = request_json("POST", "/api/pipelines/silver/run", timeout=30)
    wait_for_silver(payload["run"]["airflow_run_id"], logger)


def parse_timestamp(value: Any) -> bool:
    if value in (None, ""):
        return False
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def main() -> None:
    logger = setup_logging("validate_silver_quality")
    ensure_silver(logger)

    tables = request_json("GET", "/api/silver/tables", timeout=60)
    names = {table["silver_table_name"] for table in tables}
    assert_ok(names, "No dynamic Silver tables are available")

    for table_name in sorted(names):
        preview = request_json("GET", "/api/silver/preview", params={"table_name": table_name, "limit": 5}, timeout=60)
        assert_ok(preview.get("records"), f"{table_name} preview returned no rows")
        record = preview["records"][0]
        assert_ok("bronze_record_hash" in record, f"{table_name} missing Bronze lineage hash")
        if "created_at" in record:
            assert_ok(parse_timestamp(record["created_at"]), f"{table_name}.created_at is not normalized")
        if "updated_at" in record:
            assert_ok(parse_timestamp(record["updated_at"]), f"{table_name}.updated_at is not normalized")

    metrics = request_json("GET", "/api/silver/data-quality", timeout=60)
    assert_ok(metrics, "Silver quality metrics endpoint returned no metrics")
    duplicate_metrics = [metric for metric in metrics if metric["metric_name"] == "duplicate_primary_keys"]
    assert_ok(duplicate_metrics, "Duplicate primary key metrics are missing")
    assert_ok(all(float(metric["metric_value"]) == 0 for metric in duplicate_metrics), f"Duplicate primary keys detected: {duplicate_metrics}")
    null_key_metrics = [metric for metric in metrics if metric["metric_name"] == "null_primary_keys"]
    assert_ok(all(float(metric["metric_value"]) == 0 for metric in null_key_metrics), f"Null primary keys detected: {null_key_metrics}")

    logger.info("Silver data quality validation passed")


if __name__ == "__main__":
    main()
