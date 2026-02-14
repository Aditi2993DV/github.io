"""
Callback functions for the Zendesk Snowflake pipeline DAG.

These are referenced by the DAG's default_args and dag-level callbacks.
Extend with Slack, PagerDuty, or email integrations as needed.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def on_failure_callback(context: dict[str, Any]) -> None:
    """Called when any task in the DAG fails."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("execution_date", "unknown")
    exception = context.get("exception", "No exception captured")

    message = (
        f"[FAILURE] DAG: {dag_id} | Task: {task_id} | "
        f"Execution Date: {execution_date} | Error: {exception}"
    )
    logger.error(message)

    # --- Extend here ---
    # Example: send Slack alert
    # from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    # hook = SlackWebhookHook(slack_webhook_conn_id="slack_alerts")
    # hook.send(text=message)


def on_success_callback(context: dict[str, Any]) -> None:
    """Called when the full DAG run succeeds."""
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    execution_date = context.get("execution_date", "unknown")

    message = (
        f"[SUCCESS] DAG: {dag_id} | "
        f"Execution Date: {execution_date} | Pipeline completed."
    )
    logger.info(message)
