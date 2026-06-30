from __future__ import annotations

import os
import time
from typing import Any

import requests

from common import setup_logging


TERMINAL_STATUSES = {"success", "no_new_data", "failed"}


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    response.raise_for_status()
    return response.json()


def main() -> None:
    logger = setup_logging("trigger_bronze_pipeline")
    payload = request_json("POST", "/api/pipelines/bronze/run", timeout=30)
    airflow_run_id = payload["run"]["airflow_run_id"]
    logger.info("Triggered Bronze DAG run %s", airflow_run_id)

    deadline = time.time() + int(os.environ.get("BRONZE_RUN_TIMEOUT_SECONDS", "360"))
    latest = payload["run"]
    while time.time() < deadline:
        runs = request_json("GET", "/api/pipelines/bronze/runs", timeout=30)
        matches = [run for run in runs if run.get("airflow_run_id") == airflow_run_id]
        if matches:
            latest = matches[0]
            if latest.get("status") in TERMINAL_STATUSES:
                break
        time.sleep(5)

    logger.info("Bronze run finished status=%s rows=%s files=%s skipped=%s failed=%s", latest.get("status"), latest.get("total_rows_written"), latest.get("total_files_processed"), latest.get("total_files_skipped"), latest.get("failed_files"))
    if latest.get("status") == "failed":
        raise RuntimeError(latest.get("error_message") or "Bronze run failed")


if __name__ == "__main__":
    main()
