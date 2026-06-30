from __future__ import annotations

import argparse
import os
from urllib.parse import unquote, urlparse

from common import setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db


STARTUP_TASK_PHASES = {
    "register_bronze_metadata": "register_bronze_metadata",
    "check_bronze_updates": "check_bronze_updates / bronze validation",
    "process_silver_tables": "process_silver_tables",
    "validate_silver_layer": "validate_silver_layer",
}


def airflow_connection_parts() -> dict[str, str | int] | None:
    uri = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN") or ""
    if not uri:
        return None
    parsed = urlparse(uri.replace("postgresql+psycopg2://", "postgresql://", 1))
    if parsed.scheme not in {"postgresql", "postgres"} or not parsed.hostname or not parsed.path:
        return None
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
    }


def failed_airflow_task(airflow_run_id: str) -> tuple[str, str] | None:
    parts = airflow_connection_parts()
    if not parts:
        return None
    try:
        import psycopg2
        from psycopg2.extras import DictCursor

        with psycopg2.connect(**parts) as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT task_id, state
                    FROM task_instance
                    WHERE dag_id = %s
                      AND run_id = %s
                      AND state IN ('failed', 'upstream_failed')
                    ORDER BY
                      CASE WHEN state = 'failed' THEN 0 ELSE 1 END,
                      start_date NULLS LAST,
                      task_id
                    LIMIT 1
                    """,
                    (os.environ.get("AIRFLOW_CTX_DAG_ID") or "silver_processing_pipeline", airflow_run_id),
                )
                row = cursor.fetchone()
                if row:
                    return str(row["task_id"]), str(row["state"])
    except Exception:
        return None
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", default="success")
    parser.add_argument("--message", default=None)
    args = parser.parse_args()

    logger = setup_logging("update_dashboard_silver_status")
    init_dashboard_db()
    airflow_run_id = os.environ.get("AIRFLOW_SILVER_RUN_ID") or os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
    if not airflow_run_id:
        logger.info("No Airflow Silver run id in environment; nothing to update")
        return

    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, status
                FROM silver_processing_runs
                WHERE airflow_run_id = %s
                """,
                (airflow_run_id,),
            )
            run = cursor.fetchone()
            if not run:
                logger.info("No Silver dashboard run found for %s", airflow_run_id)
                return
            run_id, current_status = run
            terminal_statuses = {"success", "no_new_data", "warning", "unmapped_bronze_tables", "failed", "cancelled"}
            effective_status = args.status
            effective_message = args.message
            effective_phase = None
            if args.status == "success" and current_status not in terminal_statuses:
                cursor.execute(
                    """
                    SELECT
                        count(*) FILTER (WHERE status = 'failed') AS failed_batches,
                        count(*) FILTER (WHERE status IN ('queued', 'processing', 'retrying')) AS unfinished_batches
                    FROM silver_processing_batches
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                failed_batches, unfinished_batches = cursor.fetchone()
                if int(failed_batches or 0) > 0:
                    effective_status = "failed"
                    effective_message = effective_message or f"Silver DAG completed but {failed_batches} batch(es) failed."
                elif int(unfinished_batches or 0) > 0:
                    effective_status = "failed"
                    effective_message = effective_message or f"Silver DAG completed with {unfinished_batches} unfinished batch state(s). Mark interrupted or retry failed Silver."
                else:
                    failed_task = failed_airflow_task(airflow_run_id)
                    if failed_task is not None:
                        task_id, task_state = failed_task
                        effective_status = "failed"
                        effective_phase = STARTUP_TASK_PHASES.get(task_id, task_id)
                        effective_message = effective_message or f"Silver DAG stopped before processing completed: task {task_id} {task_state}."
            cursor.execute(
                """
                UPDATE silver_processing_runs
                SET status = CASE
                        WHEN status IN ('success', 'no_new_data', 'warning', 'unmapped_bronze_tables', 'failed', 'cancelled') THEN status
                        WHEN status = 'running' THEN %s
                        ELSE 'failed'
                    END,
                    error_message = CASE
                        WHEN status IN ('success', 'no_new_data', 'warning', 'unmapped_bronze_tables', 'failed', 'cancelled') THEN error_message
                        WHEN status = 'running' THEN COALESCE(error_message, %s)
                        ELSE COALESCE(error_message, %s, 'Silver processing task did not start or did not update run state')
                    END,
                    current_phase = CASE
                        WHEN status IN ('success', 'no_new_data', 'warning', 'unmapped_bronze_tables', 'failed', 'cancelled') THEN current_phase
                        WHEN %s IS NOT NULL THEN %s
                        ELSE current_phase
                    END,
                    finished_at = CASE
                        WHEN status IN ('success', 'no_new_data', 'warning', 'unmapped_bronze_tables', 'failed', 'cancelled') THEN COALESCE(finished_at, now())
                        ELSE now()
                    END,
                    duration_seconds = COALESCE(duration_seconds, EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now())))),
                    updated_at = now()
                WHERE airflow_run_id = %s
                """,
                (effective_status, effective_message, effective_message, effective_phase, effective_phase, airflow_run_id),
            )
    logger.info("Silver dashboard run status updated for %s", airflow_run_id)


if __name__ == "__main__":
    main()
