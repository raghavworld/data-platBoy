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


silver_env = {
    "AIRFLOW_SILVER_RUN_ID": "{{ run_id }}",
    "ONOV8_TRIGGERED_BY": "{{ dag_run.conf.get('triggered_by', 'airflow') if dag_run else 'airflow' }}",
    "SILVER_DASHBOARD_RUN_ID": "{{ dag_run.conf.get('dashboard_run_id', '') if dag_run else '' }}",
    "SILVER_SCOPE": "{{ dag_run.conf.get('scope', 'all_pending') if dag_run else 'all_pending' }}",
    "SILVER_DATABASE_NAME": "{{ dag_run.conf.get('database_name', '') if dag_run else '' }}",
    "SILVER_COLLECTION_NAME": "{{ dag_run.conf.get('collection_name', '') if dag_run else '' }}",
    "SILVER_TABLE_NAME": "{{ dag_run.conf.get('table_name', '') if dag_run else '' }}",
    "SILVER_BRONZE_TABLE": "{{ dag_run.conf.get('bronze_table', '') if dag_run else '' }}",
    "SILVER_BRONZE_FILE": "{{ dag_run.conf.get('bronze_file', '') if dag_run else '' }}",
    "SILVER_RETRY_FAILED": "{{ dag_run.conf.get('retry_failed', False) if dag_run else False }}",
    "SILVER_TARGET_TABLES": "{{ dag_run.conf.get('target_tables', '') if dag_run else '' }}",
    "SILVER_BATCH_SIZE": "{{ dag_run.conf.get('silver_batch_size', '50') if dag_run else '50' }}",
    "SILVER_PARALLEL_COLLECTIONS": "{{ dag_run.conf.get('silver_parallel_collections', '1') if dag_run else '1' }}",
    "SILVER_PARALLEL_TABLES": "{{ dag_run.conf.get('silver_parallel_tables', '1') if dag_run else '1' }}",
}


with DAG(
    dag_id="silver_processing_pipeline",
    default_args=default_args,
    description="Phase 3 Silver processing from Bronze Delta tables into clean analytics Delta tables",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["onov8", "silver", "delta", "phase-3"],
) as dag:
    check_bronze_updates = BashOperator(
        task_id="check_bronze_updates",
        bash_command="python /opt/platform/scripts/validate_bronze_tables.py",
        env=silver_env,
        append_env=True,
    )

    register_bronze_metadata = BashOperator(
        task_id="register_bronze_metadata",
        bash_command="python /opt/platform/scripts/register_bronze_tables.py",
        env=silver_env,
        append_env=True,
    )

    process_silver_tables = BashOperator(
        task_id="process_silver_tables",
        bash_command=(
            "spark-submit --master local[2] "
            "--packages io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/platform/scripts/silver_bronze_to_delta.py"
        ),
        env=silver_env,
        append_env=True,
    )

    validate_silver_layer = BashOperator(
        task_id="validate_silver_layer",
        bash_command="python /opt/platform/scripts/validate_silver_tables.py",
        env=silver_env,
        append_env=True,
    )

    update_dashboard_silver_status = BashOperator(
        task_id="update_dashboard_silver_status",
        bash_command="python /opt/platform/scripts/update_dashboard_silver_status.py --status success",
        env=silver_env,
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    register_bronze_metadata >> check_bronze_updates >> process_silver_tables >> validate_silver_layer >> update_dashboard_silver_status
