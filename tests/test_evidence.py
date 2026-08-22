from pathlib import Path

import pytest

from orbit.evidence import collect
from orbit.store.repository import Repository

DAGS = Path("dags")


class MockClient:
    def __init__(self, successful=None, outputs=None, log_lines=500, params=None):
        self.successful = successful or []
        self.outputs = outputs or {}
        self.log_lines = log_lines
        self.params = params if params is not None else {"orbit_replayable": True}

    def get_dag_params(self, dag_id):
        return self.params

    def get_task_log(self, dag_id, run_id, task_id, try_number):
        return [f"line {i}" for i in range(self.log_lines)]

    def list_successful_runs(self, dag_id, task_id, limit):
        return self.successful[:limit]

    def get_xcom(self, dag_id, run_id, task_id, key="return_value"):
        if (run_id, task_id) not in self.outputs:
            raise KeyError(f"no xcom for {run_id}/{task_id}")
        return self.outputs[(run_id, task_id)]


class MockBrokenLogClient(MockClient):
    def get_task_log(self, dag_id, run_id, task_id, try_number):
        raise RuntimeError("log server down")


def _repo_with_incident(tmp_path) -> tuple[Repository, str]:
    repo = Repository(tmp_path / "orbit.db")
    incident_id = repo.create_incident(
        "retail_etl", "clean_orders", "run-bad", 1, "KeyError", "'customer_id'"
    )
    return repo, incident_id


def test_collect_truncates_log_tail(tmp_path):
    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, MockClient(), DAGS)
    assert len(evidence.log_tail) == 200
    assert evidence.log_tail[-1] == "line 499"


def test_collect_keeps_short_logs_intact(tmp_path):
    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, MockClient(log_lines=5), DAGS)
    assert len(evidence.log_tail) == 5


def test_collect_reads_source_of_the_failing_dag(tmp_path):
    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, MockClient(), DAGS)
    assert evidence.source_path.endswith("retail_etl.py")
    assert "def clean_orders_fn" in evidence.source_code


def test_collect_builds_regression_cases_from_successful_runs(tmp_path):
    client = MockClient(
        successful=["run-1", "run-2"],
        outputs={
            ("run-1", "clean_orders"): [{"customer_id": "C001"}],
            ("run-1", "extract_orders"): [{"customer_id": "C001"}],
            ("run-2", "clean_orders"): [{"customer_id": "C002"}],
            ("run-2", "extract_orders"): [{"customer_id": "C002"}],
        },
    )
    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, client, DAGS)
    assert len(evidence.regression_cases) == 2
    assert evidence.regression_cases[0].run_id == "run-1"
    assert evidence.regression_cases[0].expected_output == [{"customer_id": "C001"}]
    assert evidence.regression_cases[0].inputs == {"rows": [{"customer_id": "C001"}]}


def test_collect_skips_cases_with_missing_xcom(tmp_path):
    client = MockClient(
        successful=["run-1", "run-2"],
        outputs={
            ("run-1", "clean_orders"): [{"customer_id": "C001"}],
            ("run-1", "extract_orders"): [{"customer_id": "C001"}],
        },
    )
    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, client, DAGS)
    assert len(evidence.regression_cases) == 1


def test_collect_survives_missing_regression_history(tmp_path):
    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, MockClient(successful=[]), DAGS)
    assert evidence.regression_cases == []
    assert evidence.exception_type == "KeyError"


def test_collect_survives_unreachable_log_server(tmp_path):
    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, MockBrokenLogClient(), DAGS)
    assert evidence.log_tail == []
    assert evidence.exception_type == "KeyError"


def test_collect_captures_failing_inputs_from_upstream(tmp_path):
    client = MockClient(
        outputs={("run-bad", "extract_orders"): [{"cust_id": "C001"}]},
    )
    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, client, DAGS)
    assert evidence.failing_inputs == {"rows": [{"cust_id": "C001"}]}


def test_collect_reads_replayable_and_volatile_from_dag_params(tmp_path):
    repo, incident_id = _repo_with_incident(tmp_path)
    client = MockClient(
        params={"orbit_replayable": True, "orbit_volatile_fields": ["processed_at"]}
    )
    evidence = collect(incident_id, repo, client, DAGS)
    assert evidence.replayable is True
    assert evidence.volatile_fields == ["processed_at"]


def test_collect_defaults_to_not_replayable(tmp_path):
    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, MockClient(params={}), DAGS)
    assert evidence.replayable is False
    assert evidence.volatile_fields == []


def test_unreadable_params_mean_not_replayable(tmp_path):
    """Failing closed: if we cannot confirm opt-in, we do not replay."""

    class MockNoParamsClient(MockClient):
        def get_dag_params(self, dag_id):
            raise RuntimeError("api down")

    repo, incident_id = _repo_with_incident(tmp_path)
    evidence = collect(incident_id, repo, MockNoParamsClient(), DAGS)
    assert evidence.replayable is False


def test_collect_raises_on_unknown_incident(tmp_path):
    repo = Repository(tmp_path / "orbit.db")
    with pytest.raises(ValueError):
        collect("inc-nope", repo, MockClient(), DAGS)
