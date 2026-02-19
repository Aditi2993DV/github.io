"""
YAML-driven DAG generator.

This file is the Airflow entrypoint. It delegates all DAG construction to the
factory module which reads ``pipelines/*.yml`` and builds one DAG per file.

To add a new pipeline, create a new YAML file in ``pipelines/`` — no Python
changes required.
"""

from factory.dag_factory import generate_dags

# Register every generated DAG in module-level globals so that
# Airflow's DagBag discovers them automatically.
_generated = generate_dags()
globals().update(_generated)
