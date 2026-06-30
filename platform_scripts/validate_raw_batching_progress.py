#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from typing import Any

import requests

from common import load_environment
from dashboard_db import (
    dashboard_connection,
    ensure_raw_run,
    finish_raw_run,
    init_dashboard_db,
    new_id,
    record_raw_run_timing,
    update_raw_run_progress,
)


API_BASE = os.environ.get("DASHBOARD_API_BASE_URL") or f"http://localhost:{os.environ.get('DASHBOARD_API_PORT', '8001')}"


def assert_ok(condition: bool, message: str, details: Any | None = None) -> None:
    if not condition:
        raise RuntimeError(f"{message}: {details}" if details is not None else message)


def request_json(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{API_BASE.rstrip('/')}{path}", timeout=kwargs.pop("timeout", 30), **kwargs)
    response.raise_for_status()
    return response.json()


def insert_batch(
    *,
    run_id: str,
    database_name: str,
    collection_name: str,
    batch_number: int,
    total_batches: int,
    status: str,
    started_minutes_ago: int | None = None,
) -> str:
    batch_id = new_id()
    started_sql = "now() - (%s || ' minutes')::interval" if started_minutes_ago is not None else "NULL"
    params: list[Any] = [
        batch_id,
        run_id,
        database_name,
        collection_name,
        batch_number,
        total_batches,
        1000,
        status,
    ]
    if started_minutes_ago is not None:
        params.append(started_minutes_ago)
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO raw_ingestion_batches (
                    batch_id, run_id, source_id, database_name, collection_name,
                    batch_number, total_batches, estimated_rows, status,
                    started_at, cursor_start, cursor_end, retryable
                )
                VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, {started_sql}, 'cursor-a', 'cursor-b', true)
                """,
                params,
            )
    return batch_id


def batch_row(batch_id: str) -> dict[str, Any]:
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT batch_id::text, run_id::text, status, error_type, error_message,
                       retryable, recommended_fix, batch_number, cursor_start, cursor_end
                FROM raw_ingestion_batches
                WHERE batch_id = %s
                """,
                (batch_id,),
            )
            row = cursor.fetchone()
    assert_ok(row is not None, "Validation batch disappeared", batch_id)
    keys = ["batch_id", "run_id", "status", "error_type", "error_message", "retryable", "recommended_fix", "batch_number", "cursor_start", "cursor_end"]
    return dict(zip(keys, row))


def cleanup(run_ids: list[str]) -> None:
    if not run_ids:
        return
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM raw_ingestion_runs WHERE id = ANY(%s::uuid[])", (run_ids,))


def main() -> int:
    load_environment()
    init_dashboard_db()
    run_ids: list[str] = []
    checks: list[dict[str, Any]] = []
    try:
        active_airflow_run_id = f"raw_batching_validation_active_{new_id()}"
        active_run_id = ensure_raw_run(active_airflow_run_id, triggered_by="validation")
        run_ids.append(active_run_id)
        insert_batch(
            run_id=active_run_id,
            database_name="validation_raw_batching",
            collection_name="orders",
            batch_number=1,
            total_batches=3,
            status="success",
        )
        running_batch_id = insert_batch(
            run_id=active_run_id,
            database_name="validation_raw_batching",
            collection_name="orders",
            batch_number=2,
            total_batches=3,
            status="running",
            started_minutes_ago=10,
        )
        queued_batch_id = insert_batch(
            run_id=active_run_id,
            database_name="validation_raw_batching",
            collection_name="orders",
            batch_number=3,
            total_batches=3,
            status="queued",
        )
        update_raw_run_progress(
            active_run_id,
            status="running",
            phase="extracting",
            total_databases=1,
            completed_databases=0,
            total_collections=1,
            completed_collections=0,
            total_estimated_records=3000,
            processed_records=1250,
            total_batches=3,
            completed_batches=1,
            current_database="validation_raw_batching",
            current_collection="orders",
            current_batch_number=2,
            progress_message="Validation RAW run in progress",
        )

        active_payload = request_json("GET", "/api/raw/runs/active", timeout=20)
        assert_ok(isinstance(active_payload.get("runs"), list), "Active progress endpoint did not return a runs list", active_payload)
        progress = request_json("GET", f"/api/raw/runs/{active_airflow_run_id}/progress", timeout=20)
        assert_ok(progress.get("status") in {"running", "cancelling"}, "Progress endpoint did not restore running state", progress)
        assert_ok(float(progress.get("progress_percent") or 0) > 0, "Running RAW progress was not greater than zero", progress)
        assert_ok(progress.get("current_phase") == "extracting", "Progress endpoint did not expose current phase", progress)
        assert_ok(progress.get("current_database") == "validation_raw_batching", "Progress endpoint did not expose current database", progress)
        assert_ok(progress.get("current_collection") == "orders", "Progress endpoint did not expose current collection", progress)
        assert_ok(progress.get("current_batch_number") == 2, "Progress endpoint did not expose current batch", progress)
        assert_ok("latest_event" in progress, "Progress endpoint is missing latest_event", progress)
        checks.append({"status": "ok", "message": "active progress endpoint restores persistent running state"})

        active_events = request_json("GET", f"/api/raw/runs/{active_airflow_run_id}/events", timeout=20)
        active_event_types = {event.get("event_type") for event in active_events.get("events", [])}
        assert_ok({"queued", "started"} <= active_event_types, "Small run lifecycle did not record queued/started events", active_events)
        checks.append({"status": "ok", "message": "RAW event history records queued and started lifecycle"})

        paged_batches = request_json("GET", f"/api/raw/runs/{active_airflow_run_id}/batches?limit=1&offset=1", timeout=20)
        assert_ok(len(paged_batches.get("batches") or []) == 1, "Batch pagination did not return exactly one row", paged_batches)
        checks.append({"status": "ok", "message": "batch pagination endpoint works"})

        interrupted = request_json("POST", f"/api/raw/runs/{active_airflow_run_id}/mark-interrupted?timeout_minutes=1", timeout=20)
        assert_ok(int(interrupted.get("interrupted_batches") or 0) >= 1, "Interrupted endpoint did not mark the old running batch", interrupted)
        interrupted_batch = batch_row(running_batch_id)
        assert_ok(
            interrupted_batch["status"] == "failed" and interrupted_batch["error_type"] == "Interrupted" and interrupted_batch["retryable"],
            "Interrupted batch was not marked failed/retryable with exact metadata",
            interrupted_batch,
        )
        checks.append({"status": "ok", "message": "interruption marks the exact batch retryable"})

        cancelled = request_json("POST", f"/api/raw/runs/{active_airflow_run_id}/cancel", timeout=20)
        assert_ok(cancelled.get("status") == "cancelling", "Cancel running RAW did not enter cancelling state", cancelled)
        queued_batch = batch_row(queued_batch_id)
        assert_ok(
            queued_batch["status"] == "cancelled" and queued_batch["error_type"] == "UserCancelled" and not queued_batch["retryable"],
            "Cancel running RAW did not cancel queued batch safely",
            queued_batch,
        )
        checks.append({"status": "ok", "message": "cancel running RAW cancels queued batches safely"})

        success_airflow_run_id = f"raw_batching_validation_success_{new_id()}"
        success_run_id = ensure_raw_run(success_airflow_run_id, triggered_by="validation")
        run_ids.append(success_run_id)
        insert_batch(
            run_id=success_run_id,
            database_name="validation_raw_batching",
            collection_name="customers",
            batch_number=1,
            total_batches=1,
            status="success",
        )
        update_raw_run_progress(success_run_id, status="running", phase="writing raw file", total_collections=1, total_batches=1)
        record_raw_run_timing(
            run_id=success_run_id,
            level="collection",
            database_name="validation_raw_batching",
            collection_name="customers",
            source_connect_seconds=0.2,
            query_seconds=0.3,
            file_write_seconds=0.4,
            metadata_update_seconds=0.5,
            total_duration_seconds=1.4,
            warning_message="Run processed 3 rows but took 18s. Most time was spent in startup/metadata update.",
        )
        finish_raw_run(success_run_id, "success", 10, 10, 1, 0, 0)
        success_progress = request_json("GET", f"/api/raw/runs/{success_airflow_run_id}/progress", timeout=20)
        assert_ok(float(success_progress.get("progress_percent") or 0) == 100, "Completed RAW did not reach 100%", success_progress)
        assert_ok(success_progress.get("current_phase") == "completed", "Completed RAW did not expose completed phase", success_progress)
        checks.append({"status": "ok", "message": "completed RAW progress reaches 100%"})

        latest_payload = request_json("GET", "/api/raw/runs/latest", timeout=20)
        assert_ok(latest_payload.get("run", {}).get("airflow_run_id") == success_airflow_run_id, "Latest completed run endpoint did not return the newest run", latest_payload)
        active_after_success = request_json("GET", "/api/raw/runs/active", timeout=20)
        assert_ok(
            any(run.get("airflow_run_id") == success_airflow_run_id for run in active_after_success.get("runs", [])),
            "Latest completed run did not remain visible from active progress endpoint",
            active_after_success,
        )
        success_events = request_json("GET", f"/api/raw/runs/{success_airflow_run_id}/events", timeout=20)
        success_event_types = [event.get("event_type") for event in success_events.get("events", [])]
        assert_ok("queued" in success_event_types and "started" in success_event_types and "completed" in success_event_types, "Completed lifecycle did not record queued/started/completed", success_events)
        timings = request_json("GET", f"/api/raw/runs/{success_airflow_run_id}/timings", timeout=20)
        assert_ok(timings.get("timings"), "Timing breakdown endpoint returned no rows", timings)
        assert_ok(float(timings.get("summary", {}).get("metadata_update_seconds") or 0) > 0, "Timing summary did not include metadata update time", timings)
        assert_ok(timings.get("summary", {}).get("warning_message"), "Timing endpoint did not expose small-run warning guidance", timings)
        checks.append({"status": "ok", "message": "latest run, event history, and timing breakdown endpoints work"})

        failed_airflow_run_id = f"raw_batching_validation_failed_{new_id()}"
        failed_run_id = ensure_raw_run(failed_airflow_run_id, triggered_by="validation")
        run_ids.append(failed_run_id)
        insert_batch(
            run_id=failed_run_id,
            database_name="validation_raw_batching",
            collection_name="payments",
            batch_number=1,
            total_batches=2,
            status="failed",
        )
        update_raw_run_progress(failed_run_id, status="running", phase="extracting", total_collections=1, total_batches=2)
        record_raw_run_timing(
            run_id=failed_run_id,
            level="batch",
            database_name="validation_raw_batching",
            collection_name="payments",
            query_seconds=0.2,
            total_duration_seconds=0.2,
            warning_message="Validation failed batch timing",
        )
        finish_raw_run(failed_run_id, "failed", 50, 25, 1, 0, 0, "Validation failure")
        failed_progress = request_json("GET", f"/api/raw/runs/{failed_airflow_run_id}/progress", timeout=20)
        assert_ok(0 < float(failed_progress.get("progress_percent") or 0) < 100, "Failed RAW did not show partial progress", failed_progress)
        checks.append({"status": "ok", "message": "failed RAW shows partial progress"})

        request_json("GET", "/api/raw/files?limit=1&offset=0", timeout=20)
        checks.append({"status": "ok", "message": "raw files pagination endpoint accepts limit and offset"})

        print(json.dumps({"status": "ok", "checks": checks}, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        cleanup(run_ids)


if __name__ == "__main__":
    raise SystemExit(main())
