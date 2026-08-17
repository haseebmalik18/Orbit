import pytest
import requests

from orbit.trigger import AirflowClient, TriggerFailed


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


def test_trigger_dag_posts_logical_date_key(monkeypatch):
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return FakeResponse(200, {"dag_run_id": "manual__2026-08-13"})

    monkeypatch.setattr(requests, "post", fake_post)
    client = AirflowClient("http://af/api/v2", "tok")
    run_id = client.trigger_dag("orbit_remediation", {"incident_id": "inc-1"})

    assert run_id == "manual__2026-08-13"
    assert captured["url"] == "http://af/api/v2/dags/orbit_remediation/dagRuns"
    # must be present even when null; omitting it is a 422
    assert "logical_date" in captured["json"]
    assert captured["json"]["logical_date"] is None
    assert captured["json"]["conf"] == {"incident_id": "inc-1"}
    assert captured["headers"]["Authorization"] == "Bearer tok"


def test_trailing_slash_in_base_url_does_not_double_up(monkeypatch):
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["url"] = url
        return FakeResponse(200, {"dag_run_id": "x"})

    monkeypatch.setattr(requests, "post", fake_post)
    AirflowClient("http://af/api/v2/", "tok").trigger_dag("d", {})
    assert captured["url"] == "http://af/api/v2/dags/d/dagRuns"


def test_trigger_dag_raises_on_http_error(monkeypatch):
    monkeypatch.setattr(
        requests, "post", lambda *a, **k: FakeResponse(403, {"detail": "forbidden"})
    )
    with pytest.raises(TriggerFailed):
        AirflowClient("http://af/api/v2", "tok").trigger_dag("d", {})


def test_trigger_dag_raises_on_connection_error(monkeypatch):
    def explode(*args, **kwargs):
        raise requests.ConnectionError("refused")

    monkeypatch.setattr(requests, "post", explode)
    with pytest.raises(TriggerFailed):
        AirflowClient("http://af/api/v2", "tok").trigger_dag("d", {})


def test_get_xcom_returns_value(monkeypatch):
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: FakeResponse(200, {"value": {"row_count": 8}})
    )
    client = AirflowClient("http://af/api/v2", "tok")
    assert client.get_xcom("d", "r", "t") == {"row_count": 8}


def test_list_successful_runs_returns_run_ids_newest_first(monkeypatch):
    payload = {
        "dag_runs": [
            {"dag_run_id": "run-c"},
            {"dag_run_id": "run-b"},
            {"dag_run_id": "run-a"},
        ]
    }
    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(200, payload))
    client = AirflowClient("http://af/api/v2", "tok")
    assert client.list_successful_runs("d", "t", limit=3) == ["run-c", "run-b", "run-a"]


def test_get_task_log_splits_string_content(monkeypatch):
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: FakeResponse(200, {"content": "a\nb\nc"})
    )
    client = AirflowClient("http://af/api/v2", "tok")
    assert client.get_task_log("d", "r", "t", 1) == ["a", "b", "c"]


def test_get_task_log_handles_list_content(monkeypatch):
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: FakeResponse(200, {"content": ["a", "b"]})
    )
    client = AirflowClient("http://af/api/v2", "tok")
    assert client.get_task_log("d", "r", "t", 1) == ["a", "b"]
