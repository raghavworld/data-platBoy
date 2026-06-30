from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "onov8-data-console",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


governance_env = {
    "AIRFLOW_GOVERNANCE_RUN_ID": "{{ run_id }}",
    "ONOV8_TRIGGERED_BY": "{{ dag_run.conf.get('triggered_by', 'airflow') if dag_run else 'airflow' }}",
    "DASHBOARD_GOVERNANCE_RUN_ID": "{{ dag_run.conf.get('dashboard_run_id', '') if dag_run else '' }}",
}


with DAG(
    dag_id="governance_metadata_pipeline",
    default_args=default_args,
    description="Phase 6 OpenMetadata and ONOV8 governance metadata sync",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["onov8", "governance", "metadata", "phase-6"],
) as dag:
    check_openmetadata_health = BashOperator(
        task_id="check_openmetadata_health",
        bash_command="python /opt/platform/scripts/governance_metadata.py check-openmetadata-health",
        env=governance_env,
        append_env=True,
    )

    sync_trino_metadata = BashOperator(
        task_id="sync_trino_metadata",
        bash_command="python /opt/platform/scripts/governance_metadata.py sync-trino-metadata",
        env=governance_env,
        append_env=True,
    )

    sync_superset_metadata_if_available = BashOperator(
        task_id="sync_superset_metadata_if_available",
        bash_command="python /opt/platform/scripts/governance_metadata.py sync-superset-metadata-if-available",
        env=governance_env,
        append_env=True,
    )

    apply_pii_tags = BashOperator(
        task_id="apply_pii_tags",
        bash_command="python /opt/platform/scripts/governance_metadata.py apply-pii-tags",
        env=governance_env,
        append_env=True,
    )

    refresh_lineage = BashOperator(
        task_id="refresh_lineage",
        bash_command="python /opt/platform/scripts/governance_metadata.py refresh-lineage",
        env=governance_env,
        append_env=True,
    )

    validate_governance = BashOperator(
        task_id="validate_governance",
        bash_command="python /opt/platform/scripts/governance_metadata.py validate-governance",
        env=governance_env,
        append_env=True,
    )

    (
        check_openmetadata_health
        >> sync_trino_metadata
        >> sync_superset_metadata_if_available
        >> apply_pii_tags
        >> refresh_lineage
        >> validate_governance
    )
