from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "onov8-data-console",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


bronze_env = {
    "AIRFLOW_BRONZE_RUN_ID": "{{ run_id }}",
    "ONOV8_TRIGGERED_BY": "{{ dag_run.conf.get('triggered_by', 'airflow') if dag_run else 'airflow' }}",
    "BRONZE_DASHBOARD_RUN_ID": "{{ dag_run.conf.get('dashboard_run_id', '') if dag_run else '' }}",
    "BRONZE_SCOPE": "{{ dag_run.conf.get('scope', 'all_pending') if dag_run else 'all_pending' }}",
    "BRONZE_SOURCE_ID": "{{ dag_run.conf.get('source_id', '') if dag_run else '' }}",
    "BRONZE_DATABASE_NAME": "{{ dag_run.conf.get('database_name', '') if dag_run else '' }}",
    "BRONZE_COLLECTION_NAME": "{{ dag_run.conf.get('collection_name', '') if dag_run else '' }}",
    "BRONZE_RAW_FILE_ID": "{{ dag_run.conf.get('raw_file_id', '') if dag_run else '' }}",
    "BRONZE_RAW_OBJECT_KEY": "{{ dag_run.conf.get('raw_object_key', '') if dag_run else '' }}",
    "BRONZE_RETRY_FAILED": "{{ dag_run.conf.get('retry_failed', False) if dag_run else False }}",
    "BRONZE_BATCH_SIZE": "{{ dag_run.conf.get('bronze_batch_size', '25') if dag_run else '25' }}",
    "BRONZE_MAX_BATCH_BYTES": "{{ dag_run.conf.get('bronze_max_batch_bytes', '') if dag_run else '' }}",
    "BRONZE_DEMO_MODE": "{{ dag_run.conf.get('demo_mode', False) if dag_run else False }}",
    "BRONZE_PARALLEL_COLLECTIONS": "{{ dag_run.conf.get('bronze_parallel_collections', '1') if dag_run else '1' }}",
}


with DAG(
    dag_id="bronze_processing_pipeline",
    default_args=default_args,
    description="Phase 2 Bronze processing from Raw JSONL files into Delta Lake",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["onov8", "bronze", "delta", "phase-2"],
) as dag:
    check_raw_files = BashOperator(
        task_id="check_raw_files",
        bash_command=(
            "if [ \"$BRONZE_DEMO_MODE\" = \"True\" ] || [ \"$BRONZE_DEMO_MODE\" = \"true\" ]; then "
            "echo 'Skipping full RAW validation for demo scoped Bronze run'; "
            "else python /opt/platform/scripts/validate_raw_files.py; fi"
        ),
        env=bronze_env,
        append_env=True,
    )

    run_bronze_processing = BashOperator(
        task_id="run_bronze_processing",
        bash_command=(
            "spark-submit --master local[2] "
            "--packages io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/platform/scripts/bronze_raw_to_delta.py"
        ),
        env=bronze_env,
        append_env=True,
    )

    validate_bronze_tables = BashOperator(
        task_id="validate_bronze_tables",
        bash_command="python /opt/platform/scripts/validate_bronze_tables.py",
        env=bronze_env,
        append_env=True,
    )

    register_bronze_metadata = BashOperator(
        task_id="register_bronze_metadata",
        bash_command="python /opt/platform/scripts/register_bronze_tables.py",
        env=bronze_env,
        append_env=True,
    )

    update_dashboard_bronze_status = BashOperator(
        task_id="update_dashboard_bronze_status",
        bash_command="python /opt/platform/scripts/update_dashboard_bronze_status.py --status success",
        env=bronze_env,
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    check_raw_files >> run_bronze_processing >> register_bronze_metadata >> validate_bronze_tables >> update_dashboard_bronze_status
