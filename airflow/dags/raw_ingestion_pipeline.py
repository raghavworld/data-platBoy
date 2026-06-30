from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
default_args = {
    "owner": "onov8-data-console",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


raw_env = {
    "AIRFLOW_RAW_RUN_ID": "{{ run_id }}",
    "ONOV8_TRIGGERED_BY": "{{ dag_run.conf.get('triggered_by', 'airflow') if dag_run else 'airflow' }}",
    "ONOV8_RAW_SOURCE_ID": "{{ dag_run.conf.get('source_id', '') if dag_run else '' }}",
    "ONOV8_RAW_COLLECTION_NAME": "{{ dag_run.conf.get('collection_name', '') if dag_run else '' }}",
    "ONOV8_RAW_RETRY_OF_RUN_ID": "{{ dag_run.conf.get('retry_of_run_id', '') if dag_run else '' }}",
    "ONOV8_RAW_RETRY_REASON": "{{ dag_run.conf.get('retry_reason', '') if dag_run else '' }}",
    "ONOV8_RAW_SCHEDULED_ONLY": "{{ dag_run.conf.get('scheduled_only', 'false') if dag_run and dag_run.conf else ('true' if dag_run and dag_run.run_type == 'scheduled' else 'false') }}",
    "ONOV8_RAW_PROGRESS_STEP_DELAY_SECONDS": "{{ dag_run.conf.get('progress_step_delay_seconds', '') if dag_run else '' }}",
    "RAW_BATCH_SIZE": "{{ dag_run.conf.get('raw_batch_size', '') if dag_run else '' }}",
    "RAW_MAX_BATCHES_PER_RUN": "{{ dag_run.conf.get('raw_max_batches_per_run', '') if dag_run else '' }}",
    "RAW_PARALLEL_COLLECTIONS": "{{ dag_run.conf.get('raw_parallel_collections', '') if dag_run else '' }}",
    "RAW_PARALLEL_BATCHES": "{{ dag_run.conf.get('raw_parallel_batches', '') if dag_run else '' }}",
}


with DAG(
    dag_id="raw_ingestion_pipeline",
    default_args=default_args,
    description="Phase 1 Raw ingestion from active Mongo sources into MinIO",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["onov8", "raw", "mongo", "phase-1"],
) as dag:
    check_sources = BashOperator(
        task_id="check_sources",
        bash_command="python /opt/platform/scripts/seed_dashboard_sources.py --ensure-schema --check-active",
        env=raw_env,
        append_env=True,
    )

    run_raw_ingestion = BashOperator(
        task_id="run_raw_ingestion",
        bash_command="python /opt/platform/scripts/ingest_mongo_to_raw.py",
        env=raw_env,
        append_env=True,
    )

    validate_raw_files = BashOperator(
        task_id="validate_raw_files",
        bash_command="python /opt/platform/scripts/validate_raw_files.py",
        env=raw_env,
        append_env=True,
    )

    update_dashboard_run_status = BashOperator(
        task_id="update_dashboard_run_status",
        bash_command="python /opt/platform/scripts/update_dashboard_run_status.py --status success",
        env=raw_env,
        append_env=True,
    )

    check_sources >> run_raw_ingestion >> validate_raw_files >> update_dashboard_run_status
