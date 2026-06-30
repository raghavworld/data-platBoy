from __future__ import annotations

import argparse
import os

from common import setup_logging
from dashboard_db import dashboard_connection, init_dashboard_db


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", default="success")
    parser.add_argument("--message", default=None)
    args = parser.parse_args()

    logger = setup_logging("update_dashboard_bronze_status")
    init_dashboard_db()
    airflow_run_id = os.environ.get("AIRFLOW_BRONZE_RUN_ID") or os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
    if not airflow_run_id:
        logger.info("No Airflow Bronze run id in environment; nothing to update")
        return

    with dashboard_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bronze_processing_runs
                    SET status = CASE
                            WHEN status IN ('success', 'no_new_data', 'failed') THEN status
                            WHEN status = 'running' THEN %s
                            ELSE 'failed'
                        END,
                    current_phase = CASE
                        WHEN status IN ('success', 'no_new_data', 'failed') THEN current_phase
                        WHEN status = 'running' AND %s = 'success' THEN 'completed'
                        ELSE 'failed'
                    END,
                    error_message = CASE
                        WHEN status IN ('success', 'no_new_data', 'failed') THEN error_message
                        WHEN status = 'running' THEN COALESCE(error_message, %s)
                        ELSE COALESCE(error_message, %s, 'Bronze processing task did not start or did not update run state')
                    END,
                    finished_at = CASE
                        WHEN status IN ('success', 'no_new_data', 'failed') THEN COALESCE(finished_at, now())
                        ELSE now()
                    END,
                    duration_seconds = COALESCE(duration_seconds, EXTRACT(EPOCH FROM (now() - COALESCE(started_at, now())))),
                    updated_at = now()
                WHERE airflow_run_id = %s
                """,
                (args.status, args.status, args.message, args.message, airflow_run_id),
            )
    logger.info("Bronze dashboard run status updated for %s", airflow_run_id)


if __name__ == "__main__":
    main()
