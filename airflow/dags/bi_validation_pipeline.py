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


bi_env = {
    "AIRFLOW_BI_RUN_ID": "{{ run_id }}",
    "ONOV8_TRIGGERED_BY": "{{ dag_run.conf.get('triggered_by', 'airflow') if dag_run else 'airflow' }}",
}


with DAG(
    dag_id="bi_validation_pipeline",
    default_args=default_args,
    description="Phase 5 BI validation for Superset, Trino safe views, datasets, dashboards, and PII controls",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["onov8", "bi", "superset", "phase-5"],
) as dag:
    validate_trino_views = BashOperator(
        task_id="validate_trino_views",
        bash_command="python /opt/platform/scripts/bi_layer.py validate-trino-views",
        env=bi_env,
        append_env=True,
    )

    validate_superset_connection = BashOperator(
        task_id="validate_superset_connection",
        bash_command="python /opt/platform/scripts/bi_layer.py validate-superset-connection",
        env=bi_env,
        append_env=True,
    )

    validate_datasets = BashOperator(
        task_id="validate_datasets",
        bash_command="python /opt/platform/scripts/bi_layer.py validate-datasets",
        env=bi_env,
        append_env=True,
    )

    validate_dashboards = BashOperator(
        task_id="validate_dashboards",
        bash_command="python /opt/platform/scripts/validate_bi_dashboards.py --skip-make-check",
        env=bi_env,
        append_env=True,
    )

    update_dashboard_bi_status = BashOperator(
        task_id="update_dashboard_bi_status",
        bash_command="python /opt/platform/scripts/bi_layer.py update-dashboard-bi-status",
        env=bi_env,
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    validate_trino_views >> validate_superset_connection >> validate_datasets >> validate_dashboards >> update_dashboard_bi_status
