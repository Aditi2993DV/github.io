"""
Zendesk -> Fivetran -> dbt -> Snowflake  pipeline DAG.

Schedule: Runs every 2 hours.

Flow:
  1. Trigger Fivetran connector to sync Zendesk data into Snowflake (ZENDESK_RAW).
  2. Run dbt seed to load static reference data.
  3. Run dbt source freshness check.
  4. Run dbt staging + intermediate + mart models.
  5. Run dbt tests.

Connections required in Airflow:
  - fivetran_default  : Fivetran API key/secret (HTTP connection)
  - snowflake_default : Snowflake credentials (used by dbt via env vars)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.fivetran.operators.fivetran import FivetranOperator
from airflow.providers.fivetran.sensors.fivetran import FivetranSensor
from airflow.utils.task_group import TaskGroup

from zendesk_callbacks import (
    on_failure_callback,
    on_success_callback,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FIVETRAN_CONNECTOR_ID = "{{ var.value.zendesk_fivetran_connector_id }}"
DBT_PROJECT_DIR = "{{ var.value.dbt_project_dir }}"
DBT_PROFILES_DIR = "{{ var.value.dbt_profiles_dir }}"
DBT_TARGET = "prod"

DBT_BASE_CMD = (
    f"cd {DBT_PROJECT_DIR} && "
    f"dbt --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}"
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="zendesk_snowflake_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingest Zendesk data via Fivetran, transform with dbt, load to Snowflake",
    schedule_interval="0 */2 * * *",  # every 2 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["zendesk", "fivetran", "dbt", "snowflake"],
    on_success_callback=on_success_callback,
) as dag:

    # ------------------------------------------------------------------
    # 1. Fivetran: trigger sync and wait for completion
    # ------------------------------------------------------------------
    with TaskGroup("fivetran_sync") as fivetran_sync:
        trigger_fivetran = FivetranOperator(
            task_id="trigger_fivetran_sync",
            fivetran_conn_id="fivetran_default",
            connector_id=FIVETRAN_CONNECTOR_ID,
        )

        wait_for_fivetran = FivetranSensor(
            task_id="wait_for_fivetran_sync",
            fivetran_conn_id="fivetran_default",
            connector_id=FIVETRAN_CONNECTOR_ID,
            poke_interval=60,
            timeout=3600,
        )

        trigger_fivetran >> wait_for_fivetran

    # ------------------------------------------------------------------
    # 2. dbt: seed static reference data
    # ------------------------------------------------------------------
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_BASE_CMD} seed",
    )

    # ------------------------------------------------------------------
    # 3. dbt: check source freshness
    # ------------------------------------------------------------------
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_BASE_CMD} source freshness",
    )

    # ------------------------------------------------------------------
    # 4. dbt: run models (staging -> intermediate -> marts)
    # ------------------------------------------------------------------
    with TaskGroup("dbt_run") as dbt_run:
        dbt_run_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=f"{DBT_BASE_CMD} run --select tag:staging",
        )

        dbt_run_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=f"{DBT_BASE_CMD} run --select tag:marts",
        )

        dbt_run_staging >> dbt_run_marts

    # ------------------------------------------------------------------
    # 5. dbt: run tests
    # ------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BASE_CMD} test",
    )

    # ------------------------------------------------------------------
    # 6. Completion marker
    # ------------------------------------------------------------------
    pipeline_complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=lambda: print("Zendesk pipeline completed successfully"),
    )

    # ------------------------------------------------------------------
    # Task dependencies
    # ------------------------------------------------------------------
    fivetran_sync >> dbt_seed >> dbt_source_freshness >> dbt_run >> dbt_test >> pipeline_complete
