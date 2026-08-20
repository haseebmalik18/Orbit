from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from typing import Any

from orbit.contracts import Case

RESULT_START = "__ORBIT_RESULT__"
RESULT_END = "__ORBIT_END__"

# plain traceback form, then Airflow 3.3's structured JSON log form
TRACEBACK_EXCEPTION = re.compile(r"^(\w+(?:Error|Exception)): ", re.MULTILINE)
STRUCTURED_EXCEPTION = re.compile(r'"exc_type"\s*:\s*"(\w+)"')


@dataclass
class ReplayResult:
    succeeded: bool
    exception_type: str | None
    output: Any
    stdout: str
    stderr: str
    duration_ms: int
    timed_out: bool


def _bundle_config(shadow_root: str) -> str:
    return json.dumps(
        [
            {
                "name": "orbit-shadow",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(shadow_root)},
            }
        ]
    )


def _extract_output(stdout: str) -> Any:
    start = stdout.find(RESULT_START)
    end = stdout.find(RESULT_END)
    if start == -1 or end == -1 or end < start:
        return None
    return json.loads(stdout[start + len(RESULT_START) : end])


def _extract_exception_type(stderr: str) -> str | None:
    match = STRUCTURED_EXCEPTION.search(stderr) or TRACEBACK_EXCEPTION.search(stderr)
    return match.group(1) if match else None


def replay(
    dag_id: str,
    task_id: str,
    case: Case,
    shadow_root: str,
    timeout_s: int,
) -> ReplayResult:
    """Re-execute one task against recorded inputs inside the shadow bundle.

    Uses `airflow tasks test`, which runs a single task with no dependency
    checks and writes nothing to the metadata database.
    """
    env = {
        **os.environ,
        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": _bundle_config(shadow_root),
        "ORBIT_REPLAY_INPUTS": json.dumps(case.inputs),
        "ORBIT_REPLAY_MODE": "1",
    }
    command = ["airflow", "tasks", "test", dag_id, task_id]

    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return ReplayResult(
            succeeded=False,
            exception_type="Timeout",
            output=None,
            stdout="",
            stderr=f"replay exceeded {timeout_s}s",
            duration_ms=int((time.perf_counter() - started) * 1000),
            timed_out=True,
        )

    duration_ms = int((time.perf_counter() - started) * 1000)
    succeeded = completed.returncode == 0
    return ReplayResult(
        succeeded=succeeded,
        exception_type=None if succeeded else _extract_exception_type(completed.stderr),
        output=_extract_output(completed.stdout) if succeeded else None,
        stdout=completed.stdout,
        stderr=completed.stderr,
        duration_ms=duration_ms,
        timed_out=False,
    )
