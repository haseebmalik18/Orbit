import time
import types

import pytest

from orbit import listener
from orbit.store.repository import Repository


class MockClient:
    def __init__(self):
        self.calls = []

    def trigger_dag(self, dag_id, conf):
        self.calls.append((dag_id, conf))
        return "orbit-run-1"


class MockUnreachableClient:
    def trigger_dag(self, dag_id, conf):
        raise RuntimeError("api-server unreachable")


def _ti(dag_id="retail_etl", task_id="clean_orders"):
    return types.SimpleNamespace(
        dag_id=dag_id,
        task_id=task_id,
        run_id="run-1",
        try_number=1,
        map_index=-1,
    )


@pytest.fixture
def wired(tmp_path, monkeypatch):
    repo = Repository(tmp_path / "orbit.db")
    client = MockClient()
    monkeypatch.setattr(listener, "_repository", lambda: repo)
    monkeypatch.setattr(listener, "_client", lambda: client)
    monkeypatch.setattr(listener.settings, "watched_dag_ids", ["retail_etl"])
    return repo, client


def test_failure_creates_incident_and_triggers_remediation(wired):
    repo, client = wired
    listener.on_task_instance_failed(
        previous_state=None, task_instance=_ti(), error=KeyError("customer_id")
    )
    incidents = repo.list_incidents()
    assert len(incidents) == 1
    assert incidents[0]["exception_type"] == "KeyError"
    assert client.calls[0][0] == "orbit_remediation"
    assert client.calls[0][1]["incident_id"] == incidents[0]["id"]


def test_exception_message_is_captured(wired):
    repo, _ = wired
    listener.on_task_instance_failed(
        previous_state=None,
        task_instance=_ti(),
        error=ValueError("could not convert string to float: 'N/A'"),
    )
    assert "N/A" in repo.list_incidents()[0]["exception_message"]


def test_error_passed_as_string_is_tolerated(wired):
    repo, _ = wired
    listener.on_task_instance_failed(
        previous_state=None, task_instance=_ti(), error="something broke"
    )
    incident = repo.list_incidents()[0]
    assert incident["exception_type"] == "Unknown"
    assert incident["exception_message"] == "something broke"


def test_unwatched_dag_is_ignored(wired):
    repo, client = wired
    listener.on_task_instance_failed(
        previous_state=None, task_instance=_ti(dag_id="some_other_dag"), error=None
    )
    assert repo.list_incidents() == []
    assert client.calls == []


def test_orbit_own_dag_never_creates_incident(wired):
    repo, client = wired
    listener.on_task_instance_failed(
        previous_state=None,
        task_instance=_ti(dag_id="orbit_remediation"),
        error=RuntimeError("boom"),
    )
    assert repo.list_incidents() == []
    assert client.calls == []


def test_replay_failures_never_create_incidents(wired, monkeypatch):
    """The verifier replays failing tasks on purpose. Without this guard every
    replay opens an incident, which triggers another replay."""
    repo, client = wired
    monkeypatch.setenv("ORBIT_REPLAY_MODE", "1")
    listener.on_task_instance_failed(
        previous_state=None, task_instance=_ti(), error=KeyError("customer_id")
    )
    assert repo.list_incidents() == []
    assert client.calls == []


def test_listener_still_fires_outside_replay_mode(wired, monkeypatch):
    repo, _ = wired
    monkeypatch.delenv("ORBIT_REPLAY_MODE", raising=False)
    listener.on_task_instance_failed(
        previous_state=None, task_instance=_ti(), error=KeyError("customer_id")
    )
    assert len(repo.list_incidents()) == 1


def test_listener_swallows_its_own_exceptions(monkeypatch):
    def explode():
        raise RuntimeError("store is down")

    monkeypatch.setattr(listener, "_repository", explode)
    monkeypatch.setattr(listener.settings, "watched_dag_ids", ["retail_etl"])
    listener.on_task_instance_failed(
        previous_state=None, task_instance=_ti(), error=None
    )


def test_incident_survives_a_failed_trigger(tmp_path, monkeypatch):
    """A dead api-server must not lose the incident record."""
    repo = Repository(tmp_path / "orbit.db")
    monkeypatch.setattr(listener, "_repository", lambda: repo)
    monkeypatch.setattr(listener, "_client", lambda: MockUnreachableClient())
    monkeypatch.setattr(listener.settings, "watched_dag_ids", ["retail_etl"])
    listener.on_task_instance_failed(
        previous_state=None, task_instance=_ti(), error=KeyError("x")
    )
    assert len(repo.list_incidents()) == 1


def test_listener_completes_within_budget(wired):
    start = time.perf_counter()
    listener.on_task_instance_failed(
        previous_state=None, task_instance=_ti(), error=ValueError("x")
    )
    assert (time.perf_counter() - start) < 0.5
