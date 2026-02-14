"""
YAML-driven DAG factory for Fivetran + dbt pipelines.

How it works
------------
1. Reads every ``*.yml`` file in the ``pipelines/`` directory (sibling to this module).
2. Parses the YAML into a normalized ``PipelineConfig``.
3. Generates a fully-wired Airflow DAG per file and registers it in ``globals()``.

Adding a new pipeline
---------------------
Create a new YAML file under ``pipelines/`` — no Python changes required.
"""

from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.fivetran.operators.fivetran import FivetranOperator
from airflow.providers.fivetran.sensors.fivetran import FivetranSensor
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

PIPELINES_DIR = Path(__file__).resolve().parent.parent / "pipelines"


# ---------------------------------------------------------------------------
# Config dataclasses
# ---------------------------------------------------------------------------
@dataclass
class FivetranConnectorConfig:
    id: str
    name: str
    poke_interval_seconds: int = 60
    timeout_seconds: int = 3600


@dataclass
class FivetranConfig:
    connection_id: str
    connectors: list[FivetranConnectorConfig] = field(default_factory=list)


@dataclass
class DbtStep:
    command: str
    args: str = ""
    description: str = ""
    selectors: list[str] = field(default_factory=list)


@dataclass
class DbtConfig:
    project_dir: str
    profiles_dir: str
    target: str = "prod"
    steps: list[DbtStep] = field(default_factory=list)


@dataclass
class PipelineConfig:
    dag_id: str
    description: str
    schedule_interval: str
    start_date: datetime
    catchup: bool
    max_active_runs: int
    tags: list[str]
    default_args: dict[str, Any]
    fivetran: FivetranConfig
    dbt: DbtConfig
    on_failure_callback: Callable | None = None
    on_success_callback: Callable | None = None


# ---------------------------------------------------------------------------
# YAML -> Config parser
# ---------------------------------------------------------------------------
def _resolve_callback(spec: dict[str, str] | None) -> Callable | None:
    """Dynamically import a callback function from module.function spec."""
    if spec is None:
        return None
    module = importlib.import_module(spec["module"])
    return getattr(module, spec["function"])


def _parse_config(raw: dict[str, Any]) -> PipelineConfig:
    dag_cfg = raw["dag"]
    default_args_raw = raw.get("default_args", {})
    fivetran_raw = raw.get("fivetran", {})
    dbt_raw = raw.get("dbt", {})
    callbacks_raw = raw.get("callbacks", {})

    # Parse retry_delay from minutes
    retry_minutes = default_args_raw.pop("retry_delay_minutes", 5)
    default_args_raw["retry_delay"] = timedelta(minutes=retry_minutes)

    # Fivetran connectors
    connectors = [
        FivetranConnectorConfig(**c)
        for c in fivetran_raw.get("connectors", [])
    ]

    # dbt steps
    steps = [DbtStep(**s) for s in dbt_raw.get("steps", [])]

    # Callbacks
    on_failure = _resolve_callback(callbacks_raw.get("on_failure"))
    on_success = _resolve_callback(callbacks_raw.get("on_success"))

    if on_failure:
        default_args_raw["on_failure_callback"] = on_failure

    return PipelineConfig(
        dag_id=dag_cfg["dag_id"],
        description=dag_cfg.get("description", ""),
        schedule_interval=dag_cfg.get("schedule_interval", "@daily"),
        start_date=datetime.fromisoformat(dag_cfg.get("start_date", "2024-01-01")),
        catchup=dag_cfg.get("catchup", False),
        max_active_runs=dag_cfg.get("max_active_runs", 1),
        tags=dag_cfg.get("tags", []),
        default_args=default_args_raw,
        fivetran=FivetranConfig(
            connection_id=fivetran_raw.get("connection_id", "fivetran_default"),
            connectors=connectors,
        ),
        dbt=DbtConfig(
            project_dir=dbt_raw["project_dir"],
            profiles_dir=dbt_raw["profiles_dir"],
            target=dbt_raw.get("target", "prod"),
            steps=steps,
        ),
        on_failure_callback=on_failure,
        on_success_callback=on_success,
    )


# ---------------------------------------------------------------------------
# DAG builder
# ---------------------------------------------------------------------------
def _build_dbt_base_cmd(cfg: DbtConfig) -> str:
    return (
        f"cd {cfg.project_dir} && "
        f"dbt --profiles-dir {cfg.profiles_dir} --target {cfg.target}"
    )


def _build_fivetran_group(
    dag: DAG, fivetran_cfg: FivetranConfig
) -> list[TaskGroup]:
    """Create one TaskGroup per Fivetran connector (trigger + sensor)."""
    groups = []
    for connector in fivetran_cfg.connectors:
        with TaskGroup(f"fivetran_{connector.name}", dag=dag) as tg:
            trigger = FivetranOperator(
                task_id="trigger_sync",
                fivetran_conn_id=fivetran_cfg.connection_id,
                connector_id=connector.id,
                dag=dag,
            )
            sensor = FivetranSensor(
                task_id="wait_for_sync",
                fivetran_conn_id=fivetran_cfg.connection_id,
                connector_id=connector.id,
                poke_interval=connector.poke_interval_seconds,
                timeout=connector.timeout_seconds,
                dag=dag,
            )
            trigger >> sensor
        groups.append(tg)
    return groups


def _build_dbt_tasks(dag: DAG, dbt_cfg: DbtConfig) -> list:
    """
    Convert the dbt.steps list into Airflow tasks.

    - A step with ``selectors`` becomes a TaskGroup of sequential BashOperator
      tasks (one per selector).
    - A plain step becomes a single BashOperator.
    """
    base_cmd = _build_dbt_base_cmd(dbt_cfg)
    tasks = []

    for idx, step in enumerate(dbt_cfg.steps):
        if step.selectors:
            # Multiple selectors -> sequential sub-tasks within a TaskGroup
            with TaskGroup(f"dbt_{step.command}_{idx}", dag=dag) as tg:
                prev = None
                for sel in step.selectors:
                    safe_name = sel.replace(":", "_").replace(".", "_")
                    task = BashOperator(
                        task_id=f"dbt_{step.command}_{safe_name}",
                        bash_command=f"{base_cmd} {step.command} --select {sel}",
                        dag=dag,
                    )
                    if prev:
                        prev >> task
                    prev = task
            tasks.append(tg)
        else:
            args_part = f" {step.args}" if step.args else ""
            task = BashOperator(
                task_id=f"dbt_{step.command}_{idx}",
                bash_command=f"{base_cmd} {step.command}{args_part}",
                dag=dag,
            )
            tasks.append(task)

    return tasks


def build_dag(config: PipelineConfig) -> DAG:
    """Build a complete Airflow DAG from a PipelineConfig."""
    dag = DAG(
        dag_id=config.dag_id,
        default_args=config.default_args,
        description=config.description,
        schedule_interval=config.schedule_interval,
        start_date=config.start_date,
        catchup=config.catchup,
        max_active_runs=config.max_active_runs,
        tags=config.tags,
        on_success_callback=config.on_success_callback,
    )

    with dag:
        # -- Fivetran groups (run in parallel if multiple connectors) --
        fivetran_groups = _build_fivetran_group(dag, config.fivetran)

        # -- dbt tasks (sequential chain) --
        dbt_tasks = _build_dbt_tasks(dag, config.dbt)

        # -- Completion marker --
        pipeline_complete = PythonOperator(
            task_id="pipeline_complete",
            python_callable=lambda dag_id=config.dag_id: print(
                f"{dag_id} completed successfully"
            ),
        )

        # -- Wire dependencies --
        # All Fivetran groups must finish before the first dbt task
        first_dbt = dbt_tasks[0] if dbt_tasks else pipeline_complete
        for fg in fivetran_groups:
            fg >> first_dbt

        # Chain dbt tasks sequentially
        for i in range(len(dbt_tasks) - 1):
            dbt_tasks[i] >> dbt_tasks[i + 1]

        # Last dbt task -> completion
        if dbt_tasks:
            dbt_tasks[-1] >> pipeline_complete

    return dag


# ---------------------------------------------------------------------------
# Public: generate DAGs from all YAML files
# ---------------------------------------------------------------------------
def generate_dags() -> dict[str, DAG]:
    """
    Scan the pipelines/ directory for YAML configs and return a dict of
    {dag_id: DAG} ready for registration in the caller's globals().
    """
    dags: dict[str, DAG] = {}

    if not PIPELINES_DIR.exists():
        logger.warning("Pipelines directory not found: %s", PIPELINES_DIR)
        return dags

    for yaml_path in sorted(PIPELINES_DIR.glob("*.yml")):
        try:
            with open(yaml_path) as f:
                raw = yaml.safe_load(f)
            config = _parse_config(raw)
            dag = build_dag(config)
            dags[config.dag_id] = dag
            logger.info("Generated DAG '%s' from %s", config.dag_id, yaml_path.name)
        except Exception:
            logger.exception("Failed to generate DAG from %s", yaml_path.name)

    return dags
