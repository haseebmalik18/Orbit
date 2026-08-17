from __future__ import annotations

import logging
from typing import Any

from airflow.listeners import hookimpl

from orbit.config import settings
from orbit.store.repository import Repository
from orbit.trigger import AirflowClient

log = logging.getLogger(__name__)

REMEDIATION_DAG_ID = "orbit_remediation"


def _repository() -> Repository:
    return Repository(settings.db_path)


def _client() -> AirflowClient:
    return AirflowClient(
        settings.airflow_base_url,
        settings.airflow_api_token,
        settings.airflow_username,
        settings.airflow_password,
    )


def _describe_error(error: Any) -> tuple[str, str]:
    if isinstance(error, BaseException):
        return type(error).__name__, str(error)
    if error is None:
        return "Unknown", ""
    return "Unknown", str(error)


@hookimpl
def on_task_instance_failed(previous_state, task_instance, error) -> None:
    """Record the failure and hand off to the remediation DAG.

    Runs in the worker's task-execution context, which has no metadata-DB access
    in Airflow 3, so this only touches Orbit's SQLite file and the REST API.
    Budget is under 500ms and every exception is swallowed: a failure inside
    Orbit must never affect the user's task teardown.
    """
    try:
        dag_id = getattr(task_instance, "dag_id", None)
        if dag_id == REMEDIATION_DAG_ID:
            return
        if dag_id not in settings.watched_dag_ids:
            return

        exception_type, exception_message = _describe_error(error)
        incident_id = _repository().create_incident(
            dag_id=dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            try_number=getattr(task_instance, "try_number", 1),
            exception_type=exception_type,
            exception_message=exception_message,
        )
        log.info(
            "Orbit opened incident %s for %s.%s",
            incident_id,
            dag_id,
            task_instance.task_id,
        )
    except Exception:
        log.exception("Orbit listener failed to record incident; user task unaffected")
        return

    # separate try: a dead api-server must not lose the incident we just wrote
    try:
        _client().trigger_dag(REMEDIATION_DAG_ID, {"incident_id": incident_id})
    except Exception:
        log.exception("Orbit could not trigger remediation for %s", incident_id)
