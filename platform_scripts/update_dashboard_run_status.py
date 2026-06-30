from __future__ import annotations

import argparse
import os

from common import setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db, record_raw_run_event


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", default="success")
    parser.add_argument("--message", default=None)
    args = parser.parse_args()

    logger = setup_logging("update_dashboard_run_status")
    init_dashboard_db()
    airflow_run_id = os.environ.get("AIRFLOW_RAW_RUN_ID") or os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
    if not airflow_run_id:
        logger.info("No Airflow run id in environment; nothing to update")
        return

    row = None
    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE raw_ingestion_runs
                SET status = CASE
                        WHEN status IN ('success', 'no_new_data', 'duplicate_batch_skipped', 'failed') THEN status
                        ELSE %s
                    END,
                    finished_at = COALESCE(finished_at, now()),
                    duration_seconds = COALESCE(duration_seconds, EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now())))),
                    progress_phase = CASE
                        WHEN status IN ('success', 'no_new_data', 'duplicate_batch_skipped', 'failed') THEN progress_phase
                        WHEN %s = 'failed' THEN 'failed'
                        ELSE 'completed'
                    END,
                    progress_percent = CASE
                        WHEN status IN ('success', 'no_new_data', 'duplicate_batch_skipped') THEN progress_percent
                        WHEN %s = 'failed' THEN LEAST(99, GREATEST(1, progress_percent))
                        ELSE 100
                    END,
                    progress_updated_at = now(),
                    error_message = COALESCE(error_message, %s),
                    updated_at = now()
                WHERE airflow_run_id = %s
                """,
                (args.status, args.status, args.status, args.message, airflow_run_id),
            )
            cursor.execute("SELECT id::text, status FROM raw_ingestion_runs WHERE airflow_run_id = %s", (airflow_run_id,))
            row = cursor.fetchone()
    if row:
        record_raw_run_event(row[0], "completed" if row[1] != "failed" else "failed", args.message or f"RAW dashboard status updated to {row[1]}")
    logger.info("Dashboard run status updated for %s", airflow_run_id)


if __name__ == "__main__":
    main()
