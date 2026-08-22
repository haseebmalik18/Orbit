from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from orbit.config import settings
from orbit.contracts import Case, Evidence

log = logging.getLogger(__name__)

UPSTREAM_OF = {
    "clean_orders": "extract_orders",
    "aggregate_daily": "clean_orders",
}


def _git_diff_since_green(source_path: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "log", "-p", "-1", "--", source_path.name],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=source_path.parent,
        )
    except (subprocess.SubprocessError, OSError):
        return None
    return result.stdout or None


def collect(incident_id, repo, client, dag_source_root: Path) -> Evidence:
    """Assemble everything the agents and the verifier need.

    Regression cases come out of the metadata DB: for each recent successful
    run, the upstream task's XCom is the input and the failing task's own XCom
    is the expected output. Every past green run is a free test case.
    """
    incident = repo.get_incident(incident_id)
    if incident is None:
        raise ValueError(f"unknown incident: {incident_id}")

    dag_id, task_id = incident["dag_id"], incident["task_id"]
    source_path = Path(dag_source_root) / f"{dag_id}.py"
    source_code = source_path.read_text() if source_path.exists() else ""

    try:
        log_lines = client.get_task_log(
            dag_id, incident["run_id"], task_id, incident["try_number"]
        )
    except Exception:
        log.warning("Could not read logs for %s", incident_id)
        log_lines = []
    log_tail = log_lines[-settings.log_tail_lines :]

    upstream_task = UPSTREAM_OF.get(task_id)
    failing_inputs: dict = {}
    if upstream_task:
        try:
            failing_inputs = {
                "rows": client.get_xcom(dag_id, incident["run_id"], upstream_task)
            }
        except Exception:
            log.warning("Could not read failing inputs for %s", incident_id)

    cases: list[Case] = []
    if upstream_task:
        for run_id in client.list_successful_runs(
            dag_id, task_id, settings.regression_case_count
        ):
            try:
                cases.append(
                    Case(
                        run_id=run_id,
                        inputs={"rows": client.get_xcom(dag_id, run_id, upstream_task)},
                        expected_output=client.get_xcom(dag_id, run_id, task_id),
                    )
                )
            except Exception:
                log.warning("Skipping unusable regression case %s", run_id)

    params: dict = {}
    try:
        params = client.get_dag_params(dag_id)
    except Exception:
        log.warning("Could not read params for %s; treating as not replayable", dag_id)

    return Evidence(
        incident_id=incident_id,
        dag_id=dag_id,
        task_id=task_id,
        run_id=incident["run_id"],
        replayable=bool(params.get("orbit_replayable", False)),
        volatile_fields=list(params.get("orbit_volatile_fields") or []),
        exception_type=incident["exception_type"],
        exception_message=incident["exception_message"],
        log_tail=log_tail,
        source_path=str(source_path),
        source_code=source_code,
        git_diff_since_green=_git_diff_since_green(source_path),
        failing_inputs=failing_inputs,
        regression_cases=cases,
    )
