from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pymongo
import requests

from common import setup_logging
from dashboard_db import init_dashboard_db
from source_secrets import get_secret


TERMINAL_RAW = {"success", "no_new_data", "duplicate_batch_skipped", "failed"}
TERMINAL_BRONZE = {"success", "no_new_data", "failed"}


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    response.raise_for_status()
    return response.json()


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def wait_for_run(path: str, airflow_run_id: str, terminal: set[str], logger) -> dict[str, Any]:
    deadline = time.time() + int(os.environ.get("BRONZE_RUN_TIMEOUT_SECONDS", "420"))
    latest: dict[str, Any] = {"status": "queued", "airflow_run_id": airflow_run_id}
    while time.time() < deadline:
        runs = request_json("GET", path, timeout=30)
        matches = [run for run in runs if run.get("airflow_run_id") == airflow_run_id]
        if matches:
            latest = matches[0]
            if latest.get("status") in terminal:
                break
        time.sleep(5)
    logger.info("Run %s final status=%s", airflow_run_id, latest.get("status"))
    assert_ok(latest.get("status") in terminal - {"failed"}, f"Run failed or timed out: {latest}")
    return latest


def run_raw(logger) -> dict[str, Any]:
    payload = request_json("POST", "/api/pipelines/raw/run", timeout=30)
    return wait_for_run("/api/pipelines/runs", payload["run"]["airflow_run_id"], TERMINAL_RAW, logger)


def run_bronze(logger) -> dict[str, Any]:
    payload = request_json("POST", "/api/pipelines/bronze/run", timeout=30)
    return wait_for_run("/api/pipelines/bronze/runs", payload["run"]["airflow_run_id"], TERMINAL_BRONZE, logger)


def bronze_row_counts() -> dict[str, int]:
    tables = request_json("GET", "/api/bronze/tables", timeout=60)
    return {f"{table['database_name']}.{table['collection_name']}": int(table.get("row_count") or 0) for table in tables}


def source_uri(source: dict[str, Any]) -> str:
    secret = get_secret(source.get("secret_reference"))
    if secret.get("mongo_uri"):
        return secret["mongo_uri"]
    config = source.get("connection_config") or {}
    host = config.get("host")
    port = config.get("port", 27017)
    auth_database = source.get("auth_database") or "admin"
    return f"mongodb://{host}:{port}/{auth_database}"


def orders_source() -> dict[str, Any]:
    request_json("POST", "/api/sources/seed-demo")
    sources = request_json("GET", "/api/sources")
    matches = [source for source in sources if source["source_name"] == "orders_service"]
    assert_ok(bool(matches), "orders_service source is missing")
    return matches[0]


def raw_orders_state(source_id: str) -> dict[str, Any]:
    states = request_json("GET", "/api/raw/cursor-state")
    matches = [
        state for state in states
        if state.get("source_id") == source_id
        and state.get("database_name") == "orders_service"
        and state.get("collection_name") == "orders"
    ]
    return matches[0] if matches else {}


def insert_incremental_order(source: dict[str, Any], last_cursor_value: str | None, logger) -> str:
    if last_cursor_value:
        last_cursor = datetime.fromisoformat(last_cursor_value.replace("Z", "+00:00"))
    else:
        last_cursor = datetime.now(timezone.utc) - timedelta(minutes=5)
    new_updated_at = max(datetime.now(timezone.utc), last_cursor + timedelta(seconds=5)).replace(microsecond=0)
    order_id = f"ORD-BRONZE-INCR-{uuid.uuid4().hex[:10]}"
    document = {
        "_id": order_id,
        "customerId": "USR-1001",
        "status": "incremental_bronze_test",
        "totalAmount": 77.77,
        "currency": "AED",
        "items": [{"sku": "SKU-BRONZE-001", "qty": 1}],
        "customerEmail": "incremental.bronze@example.com",
        "customerPhone": "+971500077777",
        "extraFields": {"validation": "incremental_bronze"},
        "createdAt": new_updated_at,
        "updatedAt": new_updated_at,
    }
    with pymongo.MongoClient(source_uri(source), serverSelectionTimeoutMS=5000, tz_aware=True) as client:
        client[source["database_name"]]["orders"].insert_one(document)
    logger.info("Inserted incremental Bronze order %s updatedAt=%s", order_id, new_updated_at.isoformat())
    return order_id


def main() -> None:
    logger = setup_logging("validate_incremental_bronze")
    init_dashboard_db()

    baseline = run_bronze(logger)
    logger.info("Baseline Bronze run status=%s", baseline.get("status"))
    before = bronze_row_counts()

    first_noop = run_bronze(logger)
    second_noop = run_bronze(logger)
    after_noop = bronze_row_counts()
    assert_ok(before == after_noop, f"Bronze row counts changed without new Raw files: before={before}, after={after_noop}")
    assert_ok(int(second_noop.get("total_rows_written") or 0) == 0, "Second no-op Bronze run wrote rows")
    assert_ok(int(second_noop.get("total_files_skipped") or 0) > 0, "Second no-op Bronze run did not skip already processed raw files")

    source = orders_source()
    state = raw_orders_state(source["id"])
    insert_incremental_order(source, state.get("last_cursor_value"), logger)

    raw_run = run_raw(logger)
    assert_ok(raw_run.get("status") == "success", f"Expected Raw success after incremental insert, got {raw_run.get('status')}")
    assert_ok(int(raw_run.get("total_rows_written") or raw_run.get("total_rows") or 0) == 1, "Expected Raw to write exactly one new row")

    before_incremental_bronze = bronze_row_counts()
    bronze_run = run_bronze(logger)
    after_incremental_bronze = bronze_row_counts()
    before_orders = before_incremental_bronze.get("orders_service.orders", 0)
    after_orders = after_incremental_bronze.get("orders_service.orders", 0)
    assert_ok(after_orders == before_orders + 1, f"Expected orders Bronze row count +1, before={before_orders}, after={after_orders}")
    assert_ok(int(bronze_run.get("total_files_processed") or 0) >= 1, "Bronze did not process a new raw file")
    assert_ok(int(bronze_run.get("total_rows_written") or 0) == 1, "Bronze wrote more or fewer than the one new raw row")

    logger.info("Incremental Bronze validation passed")


if __name__ == "__main__":
    main()
