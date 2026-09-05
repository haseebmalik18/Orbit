import pytest
import requests

from orbit.trigger import AirflowClient, TriggerFailed


class MockResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


def _client(token="tok"):
    return AirflowClient("http://af", token=token)


def test_trigger_dag_posts_logical_date_key(monkeypatch):
    captured = {}

    def mock_post(url, json=None, headers=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return MockResponse(200, {"dag_run_id": "manual__2026-08-13"})

    monkeypatch.setattr(requests, "post", mock_post)
    run_id = _client().trigger_dag("orbit_remediation", {"incident_id": "inc-1"})

    assert run_id == "manual__2026-08-13"
    assert captured["url"] == "http://af/api/v2/dags/orbit_remediation/dagRuns"
    # must be present even when null; omitting it is a 422
    assert "logical_date" in captured["json"]
    assert captured["json"]["logical_date"] is None
    assert captured["json"]["conf"] == {"incident_id": "inc-1"}
    assert captured["headers"]["Authorization"] == "Bearer tok"


def test_trailing_slash_in_base_url_does_not_double_up(monkeypatch):
    captured = {}

    def mock_post(url, **kwargs):
        captured["url"] = url
        return MockResponse(200, {"dag_run_id": "x"})

    monkeypatch.setattr(requests, "post", mock_post)
    AirflowClient("http://af/", token="tok").trigger_dag("d", {})
    assert captured["url"] == "http://af/api/v2/dags/d/dagRuns"


def test_token_is_fetched_when_not_supplied(monkeypatch):
    calls = []

    def mock_post(url, json=None, headers=None, timeout=None):
        calls.append(url)
        if url.endswith("/auth/token"):
            assert json == {"username": "u", "password": "p"}
            return MockResponse(201, {"access_token": "minted"})
        assert headers["Authorization"] == "Bearer minted"
        return MockResponse(200, {"dag_run_id": "x"})

    monkeypatch.setattr(requests, "post", mock_post)
    client = AirflowClient("http://af", username="u", password="p")
    client.trigger_dag("d", {})
    assert calls[0] == "http://af/auth/token"


def test_token_is_minted_only_once(monkeypatch):
    token_calls = []

    def mock_post(url, json=None, headers=None, timeout=None):
        if url.endswith("/auth/token"):
            token_calls.append(url)
            return MockResponse(201, {"access_token": "minted"})
        return MockResponse(200, {"dag_run_id": "x"})

    monkeypatch.setattr(requests, "post", mock_post)
    client = AirflowClient("http://af", username="u", password="p")
    client.trigger_dag("d", {})
    client.trigger_dag("d", {})
    assert len(token_calls) == 1


def test_token_fetch_failure_raises_trigger_failed(monkeypatch):
    monkeypatch.setattr(
        requests, "post", lambda *a, **k: MockResponse(401, {"detail": "bad creds"})
    )
    with pytest.raises(TriggerFailed):
        AirflowClient("http://af", username="u", password="bad").trigger_dag("d", {})


def test_trigger_dag_raises_on_http_error(monkeypatch):
    monkeypatch.setattr(
        requests, "post", lambda *a, **k: MockResponse(403, {"detail": "forbidden"})
    )
    with pytest.raises(TriggerFailed):
        _client().trigger_dag("d", {})


def test_trigger_dag_raises_on_connection_error(monkeypatch):
    def explode(*args, **kwargs):
        raise requests.ConnectionError("refused")

    monkeypatch.setattr(requests, "post", explode)
    with pytest.raises(TriggerFailed):
        _client().trigger_dag("d", {})


def test_get_xcom_returns_value(monkeypatch):
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: MockResponse(200, {"value": {"row_count": 8}})
    )
    assert _client().get_xcom("d", "r", "t") == {"row_count": 8}


def test_get_xcom_hits_api_v2_path(monkeypatch):
    captured = {}

    def mock_get(url, **kwargs):
        captured["url"] = url
        return MockResponse(200, {"value": 1})

    monkeypatch.setattr(requests, "get", mock_get)
    _client().get_xcom("d", "r", "t")
    assert captured["url"] == (
        "http://af/api/v2/dags/d/dagRuns/r/taskInstances/t/xcomEntries/return_value"
    )


def test_list_successful_runs_returns_run_ids_newest_first(monkeypatch):
    payload = {
        "dag_runs": [
            {"dag_run_id": "run-c"},
            {"dag_run_id": "run-b"},
            {"dag_run_id": "run-a"},
        ]
    }
    monkeypatch.setattr(requests, "get", lambda *a, **k: MockResponse(200, payload))
    assert _client().list_successful_runs("d", "t", limit=3) == [
        "run-c",
        "run-b",
        "run-a",
    ]


def test_get_dag_params_unwraps_param_values(monkeypatch):
    """Airflow returns params as {name: {value, schema, description}}."""
    payload = {
        "params": {
            "orbit_replayable": {"value": True, "description": None},
            "orbit_volatile_fields": {"value": ["processed_at"], "description": None},
        }
    }
    monkeypatch.setattr(requests, "get", lambda *a, **k: MockResponse(200, payload))
    assert _client().get_dag_params("retail_etl") == {
        "orbit_replayable": True,
        "orbit_volatile_fields": ["processed_at"],
    }


def test_get_dag_params_tolerates_bare_values(monkeypatch):
    payload = {"params": {"orbit_replayable": True}}
    monkeypatch.setattr(requests, "get", lambda *a, **k: MockResponse(200, payload))
    assert _client().get_dag_params("d") == {"orbit_replayable": True}


def test_get_dag_params_returns_empty_when_absent(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: MockResponse(200, {}))
    assert _client().get_dag_params("d") == {}


def test_get_task_log_splits_string_content(monkeypatch):
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: MockResponse(200, {"content": "a\nb\nc"})
    )
    assert _client().get_task_log("d", "r", "t", 1) == ["a", "b", "c"]


def test_get_task_log_handles_list_content(monkeypatch):
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: MockResponse(200, {"content": ["a", "b"]})
    )
    assert _client().get_task_log("d", "r", "t", 1) == ["a", "b"]


def test_password_falls_back_to_the_shared_generated_file(tmp_path):
    """SimpleAuthManager generates a random password per container. Every
    container is pointed at one shared file so the scheduler and the API server
    agree; without this the listener cannot authenticate at all."""
    import json

    from orbit.trigger import resolve_password

    path = tmp_path / "passwords.json"
    path.write_text(json.dumps({"admin": "generated-secret"}))
    assert resolve_password("", path, "admin") == "generated-secret"


def test_an_explicit_password_wins_over_the_file(tmp_path):
    import json

    from orbit.trigger import resolve_password

    path = tmp_path / "passwords.json"
    path.write_text(json.dumps({"admin": "generated-secret"}))
    assert resolve_password("chosen", path, "admin") == "chosen"


def test_a_missing_password_file_is_not_fatal(tmp_path):
    from orbit.trigger import resolve_password

    assert resolve_password("", tmp_path / "absent.json", "admin") == ""


def test_an_unparseable_password_file_is_not_fatal(tmp_path):
    from orbit.trigger import resolve_password

    path = tmp_path / "passwords.json"
    path.write_text("not json at all")
    assert resolve_password("", path, "admin") == ""
