from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests

from common import setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILE_STATE_FIELDS = {
    "raw_file_id",
    "raw_object_key",
    "database_name",
    "collection_name",
    "raw_row_count",
    "status",
    "started_at",
    "finished_at",
    "duration_seconds",
    "failed_step",
    "error_type",
    "error_message",
    "stack_trace_summary",
    "retryable",
    "recommended_fix",
}


def api_base() -> str:
    return os.environ.get("DASHBOARD_API_BASE_URL", "http://dashboard-api:8001")


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{api_base()}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    response.raise_for_status()
    return response.json()


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def read_text(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def validate_schema_columns() -> None:
    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bronze_file_states'
                """
            )
            columns = {row[0] for row in cursor.fetchall()}
    missing = REQUIRED_FILE_STATE_FIELDS - columns
    assert_ok(not missing, f"bronze_file_states is missing observability columns: {sorted(missing)}")
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bronze_file_attempts'
                """
            )
            attempt_columns = {row[0] for row in cursor.fetchall()}
    attempt_missing = REQUIRED_FILE_STATE_FIELDS - attempt_columns
    assert_ok(not attempt_missing, f"bronze_file_attempts is missing observability columns: {sorted(attempt_missing)}")


def validate_api_visibility() -> None:
    failed_files = request_json("GET", "/api/bronze/file-states?status=failed&limit=5")
    assert_ok(isinstance(failed_files, list), "GET /api/bronze/file-states?status=failed did not return a list")
    for item in failed_files:
        missing = REQUIRED_FILE_STATE_FIELDS - set(item)
        assert_ok(not missing, f"Failed file API row is missing fields: {sorted(missing)}")

    summary = request_json("GET", "/api/bronze/errors/summary")
    for key in ["error_breakdown", "retryable_failures", "last_failure_details", "per_collection_status"]:
        assert_ok(key in summary, f"Bronze errors summary is missing {key}")


def validate_retry_endpoint() -> None:
    result = request_json("POST", "/api/bronze/retry-failed", json={"dry_run": True})
    assert_ok(result.get("status") == "dry_run", "POST /api/bronze/retry-failed dry_run did not return dry_run")
    for key in ["failed_files", "pending_files", "retryable_failed_files"]:
        assert_ok(key in result, f"Retry failed dry-run response is missing {key}")


def validate_static_guards() -> None:
    bronze_script = read_text("scripts/bronze_raw_to_delta.py")
    db_script = read_text("scripts/dashboard_db.py")
    api_script = read_text("dashboard/api/app/main.py")

    for step in [
        "read_raw",
        "parse_json",
        "add_audit_columns",
        "compute_record_hash",
        "write_delta",
        "update_metadata",
        "validate_delta",
    ]:
        assert_ok(step in bronze_script, f"Bronze processing step is not tracked: {step}")

    assert_ok("BRONZE_METADATA_DEADLOCK_RETRIES" in db_script, "Deadlock retry constant is missing")
    assert_ok("is_deadlock_error" in db_script, "Deadlock detection helper is missing")
    assert_ok("with_bronze_metadata_retry" in db_script, "Deadlock retry wrapper is missing")
    assert_ok("bronze_file_success_exists" in bronze_script, "Successful Bronze file skip guard is missing")
    assert_ok("effective_rows_written" in bronze_script, "Partial-failure row preservation guard is missing")
    assert_ok("bronze_file_attempts" in db_script, "Per-run Bronze file attempt history is missing")
    assert_ok("/api/bronze/runs/{run_id}/failed-files" in api_script, "Run failed-files endpoint is missing")
    assert_ok("/api/bronze/retry-failed" in api_script, "Retry failed files endpoint is missing")
    assert_ok("/api/bronze/errors/summary" in api_script, "Bronze errors summary endpoint is missing")


def main() -> None:
    logger = setup_logging("validate_bronze_observability")
    validate_schema_columns()
    validate_api_visibility()
    validate_retry_endpoint()
    validate_static_guards()
    logger.info("Bronze observability validation passed")


if __name__ == "__main__":
    main()
