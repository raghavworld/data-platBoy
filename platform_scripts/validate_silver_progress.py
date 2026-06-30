from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RUN_COLUMNS = {
    "scope",
    "database_name",
    "collection_name",
    "bronze_table",
    "bronze_file",
    "current_phase",
    "total_databases",
    "completed_databases",
    "total_collections",
    "completed_collections",
    "total_bronze_tables",
    "completed_bronze_tables",
    "total_batches",
    "completed_batches",
    "processed_rows",
    "bronze_read_seconds",
    "flattening_seconds",
    "child_table_generation_seconds",
    "delta_write_seconds",
    "metadata_update_seconds",
    "profiling_seconds",
    "total_timing_seconds",
    "cancel_requested",
    "progress_updated_at",
    "silver_batch_size",
    "silver_parallel_collections",
    "silver_parallel_tables",
}

BATCH_COLUMNS = {
    "batch_id",
    "run_id",
    "database_name",
    "collection_name",
    "bronze_table",
    "bronze_file",
    "batch_number",
    "total_batches",
    "rows_processed",
    "status",
    "started_at",
    "finished_at",
    "duration_seconds",
    "retry_count",
    "failed_step",
    "error_type",
    "error_message",
    "retryable",
    "recommended_fix",
}

EVENT_TYPES = {
    "queued",
    "started",
    "flattening",
    "generating_child_tables",
    "sanitizing_columns",
    "profiling_fields",
    "writing_delta",
    "generating_safe_views",
    "metadata_update",
    "completed",
    "failed",
    "cancelled",
    "retrying",
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


def assert_ok(condition: bool, message: str, details: Any | None = None) -> None:
    if not condition:
        raise RuntimeError(f"{message}: {details}" if details is not None else message)


def read_text(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def validate_static_contract() -> None:
    processor = read_text("scripts/silver_bronze_to_delta.py")
    api_script = read_text("dashboard/api/app/main.py")
    ui = read_text("dashboard/web/src/main.jsx")
    dag = read_text("airflow/dags/silver_processing_pipeline.py")

    for name in ["SILVER_BATCH_SIZE", "SILVER_PARALLEL_COLLECTIONS", "SILVER_PARALLEL_TABLES"]:
        assert_ok(name in processor and name in dag, f"{name} is not wired through processor and Airflow")

    for endpoint in [
        "/api/silver/runs/active",
        "/api/silver/runs/latest",
        "/api/silver/runs/{run_id}/progress",
        "/api/silver/runs/{run_id}/events",
        "/api/silver/runs/{run_id}/timings",
        "/api/silver/run/database/{database_name}",
        "/api/silver/run/database/{database_name}/collection/{collection_name}",
        "/api/silver/run/table/{table_name}",
        "/api/silver/run/bronze-table/{bronze_table}",
        "/api/silver/run/bronze-file/{bronze_file}",
    ]:
        assert_ok(endpoint in api_script, f"Silver progress/scope endpoint missing: {endpoint}")

    for token in [
        'scope="database"',
        'scope="collection"',
        'scope="table"',
        'scope="bronze_table"',
        'scope="bronze_file"',
        "table_name=table_name",
        'conf["table_name"]',
        "SILVER_SCOPE",
        "SILVER_DATABASE_NAME",
        "SILVER_COLLECTION_NAME",
        "SILVER_TABLE_NAME",
        "SILVER_BRONZE_TABLE",
        "SILVER_BRONZE_FILE",
    ]:
        assert_ok(token in api_script or token in dag, f"Silver exact-scope wiring missing: {token}")

    for token in [
        "fetch_bronze_tables_for_silver(",
        "fetch_successful_bronze_batches(",
        "scope=scope",
        "database_name=database_name",
        "collection_name=collection_name",
        "bronze_table=bronze_table_ref",
        "bronze_file=bronze_file_ref",
        "silver_scope_table_name()",
        "target.table_name in only_targets",
    ]:
        assert_ok(token in processor, f"Silver processor is missing scoped discovery token: {token}")

    for token in [
        "Run Silver for all pending Bronze tables",
        "Run Silver for this database",
        "Run Silver for this collection",
        "Run Silver for this table",
        "Up to date",
        "Retry Silver",
        "Scope type:",
        "Expected target Silver table(s):",
        "/api/silver/run/table/",
    ]:
        assert_ok(token in ui, f"Silver processable action UI missing: {token}")

    for event_type in EVENT_TYPES:
        assert_ok(event_type in processor or event_type in api_script, f"Silver event missing: {event_type}")

    for label in ["Silver Progress", "progress_label", "current_bronze_table", "current_bronze_file", "SilverRunProgressPanel"]:
        assert_ok(label in ui, f"Silver progress UI missing: {label}")

    for phrase in ["silver_processing_batches", "silver_run_events", "completed_batches", "progress_updated_at"]:
        assert_ok(phrase in read_text("scripts/dashboard_db.py"), f"Silver metadata contract missing: {phrase}")


def validate_schema() -> None:
    from dashboard_db import dashboard_connection, init_dashboard_db

    init_dashboard_db()
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'silver_processing_runs'
                """
            )
            run_columns = {row[0] for row in cursor.fetchall()}
            assert_ok(not (RUN_COLUMNS - run_columns), "silver_processing_runs missing columns", sorted(RUN_COLUMNS - run_columns))

            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'silver_processing_batches'
                """
            )
            batch_columns = {row[0] for row in cursor.fetchall()}
            assert_ok(not (BATCH_COLUMNS - batch_columns), "silver_processing_batches missing columns", sorted(BATCH_COLUMNS - batch_columns))

            cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'silver_run_events'")
            assert_ok(cursor.fetchone() is not None, "silver_run_events table is missing")


def cleanup_validation_rows(run_ids: list[str], silver_table_names: list[str]) -> None:
    from dashboard_db import dashboard_connection

    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            if silver_table_names:
                cursor.execute("DELETE FROM silver_batch_fingerprints WHERE silver_table_name = ANY(%s)", (silver_table_names,))
            if run_ids:
                cursor.execute("DELETE FROM silver_processing_runs WHERE id = ANY(%s::uuid[])", (run_ids,))


def validate_persistent_progress_state() -> None:
    from dashboard_db import (
        dashboard_connection,
        ensure_silver_run,
        finish_silver_processing_batch,
        finish_silver_run,
        new_id,
        queue_silver_processing_batch,
        record_silver_run_event,
        start_silver_processing_batch,
        update_silver_run_progress,
    )

    run_ids: list[str] = []
    silver_table_names: list[str] = []
    database_name = "validation_silver_progress"
    collection_name = "users"
    bronze_table = f"{database_name}__{collection_name}"
    bronze_file = "validation-progress-bronze-file.json"
    silver_table = f"{bronze_table}_clean"
    airflow_run_id = f"silver_progress_validation_{new_id()}"

    try:
        run_id = ensure_silver_run(
            airflow_run_id,
            triggered_by="validation",
            scope="collection",
            database_name=database_name,
            collection_name=collection_name,
        )
        run_ids.append(run_id)
        silver_table_names.append(silver_table)
        update_silver_run_progress(
            run_id,
            status="running",
            current_phase="writing_delta",
            current_database_name=database_name,
            current_collection_name=collection_name,
            current_bronze_table=bronze_table,
            current_bronze_file=bronze_file,
            total_databases=1,
            total_collections=1,
            total_bronze_tables=1,
            total_batches=1,
            completed_batches=0,
            processed_rows=0,
        )
        queue_silver_processing_batch(
            run_id=run_id,
            database_name=database_name,
            collection_name=collection_name,
            bronze_table=bronze_table,
            bronze_file=bronze_file,
            batch_number=1,
            total_batches=1,
        )
        queued = request_json("GET", f"/api/silver/batch-queue?run_id={airflow_run_id}&status=queued", timeout=20)
        assert_ok(queued.get("count") == 1, "Silver queue did not expose the planned queued batch", queued)
        batch_id = start_silver_processing_batch(
            run_id=run_id,
            database_name=database_name,
            collection_name=collection_name,
            bronze_table=bronze_table,
            bronze_file=bronze_file,
            batch_number=1,
            total_batches=1,
        )
        running = request_json("GET", f"/api/silver/runs/{airflow_run_id}/progress", timeout=20)
        assert_ok(running.get("status") == "running", "Persistent Silver progress did not expose running status", running)
        assert_ok(running.get("current_bronze_table") == bronze_table, "Persistent Silver progress lost current Bronze table", running)
        assert_ok(float(running.get("progress_percent") or 0) > 0, "Running Silver progress did not show non-zero progress", running)

        finish_silver_processing_batch(batch_id, "success", 17, retryable=False)
        update_silver_run_progress(
            run_id,
            status="running",
            current_phase="metadata_update",
            completed_databases=1,
            completed_collections=1,
            completed_bronze_tables=1,
            total_batches=1,
            completed_batches=1,
            total_batches_processed=1,
            total_rows_written=17,
            processed_rows=17,
            bronze_read_seconds=0.2,
            flattening_seconds=0.3,
            child_table_generation_seconds=0.1,
            delta_write_seconds=0.4,
            metadata_update_seconds=0.2,
            profiling_seconds=0.1,
            total_timing_seconds=1.3,
        )
        record_silver_run_event(
            run_id,
            "metadata_update",
            "Validation Silver metadata update event",
            database_name=database_name,
            collection_name=collection_name,
            bronze_table=bronze_table,
            bronze_file=bronze_file,
            batch_id=batch_id,
        )
        finish_silver_run(
            run_id,
            "success",
            total_bronze_batches_found=1,
            total_batches_processed=1,
            total_batches_skipped=0,
            total_rows_written=17,
            failed_batches=0,
            processed_tables=1,
            failed_tables=0,
        )

        completed = request_json("GET", f"/api/silver/runs/{airflow_run_id}/progress", timeout=20)
        refreshed = request_json("GET", f"/api/silver/runs/{airflow_run_id}/progress", timeout=20)
        assert_ok(completed.get("progress_percent") == 100, "Completed Silver progress did not reach 100%", completed)
        assert_ok(refreshed.get("progress_label") == completed.get("progress_label"), "Silver progress label did not persist across refresh", refreshed)
        latest = request_json("GET", "/api/silver/runs/latest", timeout=20)
        assert_ok(latest.get("run"), "Latest Silver run endpoint did not return a run", latest)
        overview = request_json("GET", "/api/silver/overview", timeout=20)
        assert_ok(
            overview.get("latest_completed_silver_run", {}).get("airflow_run_id") == airflow_run_id,
            "Latest completed Silver run was not visible",
            overview.get("latest_completed_silver_run"),
        )
        events = request_json("GET", f"/api/silver/runs/{airflow_run_id}/events", timeout=20)
        event_types = {event.get("event_type") for event in events.get("events", [])}
        assert_ok({"started", "metadata_update", "completed"} <= event_types, "Silver event timeline is incomplete", events)
        timings = request_json("GET", f"/api/silver/runs/{airflow_run_id}/timings", timeout=20)
        assert_ok(float(timings.get("timings", {}).get("delta_write_seconds") or 0) > 0, "Silver timing breakdown did not persist", timings)
        assert_ok(int(timings.get("batch_summary", {}).get("attempted_batches") or 0) >= 1, "Silver batch tracking summary is empty", timings)
        queue = request_json("GET", f"/api/silver/batch-queue?run_id={airflow_run_id}", timeout=20)
        assert_ok(queue.get("count") == 1, "Silver batch queue did not return the validation batch", queue)

        with dashboard_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT count(*)
                    FROM silver_processing_batches
                    WHERE run_id = %s AND status = 'success' AND rows_processed = 17
                    """,
                    (run_id,),
                )
                assert_ok(int(cursor.fetchone()[0] or 0) == 1, "Silver batch success metadata was not stored")
    finally:
        cleanup_validation_rows(run_ids, silver_table_names)


def validate_progress_api() -> None:
    active = request_json("GET", "/api/silver/runs/active")
    assert_ok("runs" in active, "Silver active runs endpoint missing runs")
    latest = request_json("GET", "/api/silver/runs/latest")
    run = active.get("run") or latest.get("run")
    if not run:
        discovery = request_json("GET", "/api/silver/bronze-discovery")
        assert_ok("databases" in discovery and "bronze_tables" in discovery, "Bronze-backed Silver discovery is incomplete")
        return
    run_id = run.get("run_id") or run.get("id") or run.get("airflow_run_id")
    progress = request_json("GET", f"/api/silver/runs/{run_id}/progress")
    for key in ["progress_percent", "progress_label", "total_batches", "completed_batches", "phase", "timings"]:
        assert_ok(key in progress, f"Silver progress response missing {key}")
    assert_ok(0 <= float(progress.get("progress_percent") or 0) <= 100, "Silver progress percent out of range")
    events = request_json("GET", f"/api/silver/runs/{run_id}/events")
    assert_ok("events" in events, "Silver events response missing events")
    timings = request_json("GET", f"/api/silver/runs/{run_id}/timings")
    assert_ok("timings" in timings and "batch_summary" in timings, "Silver timings response incomplete")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static-only", action="store_true", help="Skip live Postgres/API checks")
    args = parser.parse_args()
    logger = setup_logging("validate_silver_progress")
    validate_static_contract()
    if not args.static_only:
        validate_schema()
        validate_persistent_progress_state()
        validate_progress_api()
    logger.info("Silver progress validation passed")


if __name__ == "__main__":
    main()
