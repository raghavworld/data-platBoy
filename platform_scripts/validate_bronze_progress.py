from __future__ import annotations

import os
import argparse
import logging
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RUN_COLUMNS = {
    "current_phase",
    "total_databases",
    "completed_databases",
    "total_collections",
    "completed_collections",
    "total_raw_files",
    "completed_raw_files",
    "total_estimated_rows",
    "processed_rows",
    "read_raw_seconds",
    "parse_json_seconds",
    "write_delta_seconds",
    "metadata_update_seconds",
    "validation_seconds",
}


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    return logging.getLogger(name)


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    import requests

    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    response.raise_for_status()
    return response.json()


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def read_text(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def validate_schema() -> None:
    from dashboard_db import dashboard_connection, init_dashboard_db

    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bronze_processing_runs'
                """
            )
            run_columns = {row[0] for row in cursor.fetchall()}
            missing = RUN_COLUMNS - run_columns
            assert_ok(not missing, f"bronze_processing_runs missing progress columns: {sorted(missing)}")
            cursor.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'bronze_run_events'
                """
            )
            assert_ok(cursor.fetchone() is not None, "bronze_run_events table is missing")


def validate_progress_api() -> None:
    active = request_json("GET", "/api/bronze/runs/active")
    assert_ok("runs" in active, "Active runs endpoint missing runs")
    latest = request_json("GET", "/api/bronze/runs/latest")
    run = active.get("run") or latest.get("run")
    if not run:
        return
    run_id = run.get("run_id") or run.get("id") or run.get("airflow_run_id")
    progress = request_json("GET", f"/api/bronze/runs/{run_id}/progress")
    for key in ["progress_percent", "progress_label", "total_raw_files", "completed_raw_files", "phase"]:
        assert_ok(key in progress, f"Progress response missing {key}")
    assert_ok(0 <= float(progress.get("progress_percent") or 0) <= 100, "Progress percent out of range")
    events = request_json("GET", f"/api/bronze/runs/{run_id}/events")
    assert_ok("events" in events, "Events response missing events")
    timings = request_json("GET", f"/api/bronze/runs/{run_id}/timings")
    assert_ok("timings" in timings, "Timings response missing timings")


def validate_static_progress_paths() -> None:
    processor = read_text("scripts/bronze_raw_to_delta.py")
    api_script = read_text("dashboard/api/app/main.py")
    ui = read_text("dashboard/web/src/main.jsx")
    for phase in [
        "queued",
        "initializing",
        "reading raw file",
        "parsing json",
        "adding audit columns",
        "writing delta",
        "updating metadata",
        "validating delta",
        "completed",
        "failed",
        "skipped",
    ]:
        assert_ok(phase in processor or phase in api_script or phase in ui, f"Bronze phase missing: {phase}")
    for event in ["file_started", "file_completed", "delta_written", "metadata_updated", "validation_completed"]:
        assert_ok(event in processor, f"Bronze event missing from processor: {event}")
    for endpoint in [
        "/api/bronze/runs/active",
        "/api/bronze/runs/latest",
        "/api/bronze/runs/{run_id}/progress",
        "/api/bronze/runs/{run_id}/events",
        "/api/bronze/runs/{run_id}/timings",
    ]:
        assert_ok(endpoint in api_script, f"Progress API endpoint missing: {endpoint}")
    for label in ["Bronze Progress", "progress_label", "bronze-timing-row", "bronze-events"]:
        assert_ok(label in ui, f"Progress UI missing: {label}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-only", action="store_true", help="Skip live Postgres/API checks")
    args = parser.parse_args()
    logger = setup_logging("validate_bronze_progress")
    validate_static_progress_paths()
    if not args.static_only:
        validate_schema()
        validate_progress_api()
    logger.info("Bronze progress validation passed")


if __name__ == "__main__":
    main()
