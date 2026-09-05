import pytest

from orbit.trigger import AirflowClient, TriggerFailed


class MockResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self._payload


@pytest.fixture
def client(monkeypatch):
    c = AirflowClient(base_url="http://airflow", username="u", password="p")
    monkeypatch.setattr(c, "_mint_token", lambda: "token")
    return c


def _capture(monkeypatch, response=None):
    sent = {}

    def post(url, json=None, headers=None, timeout=None):
        sent["url"] = url
        sent["body"] = json
        return response or MockResponse()

    monkeypatch.setattr("orbit.trigger.requests.post", post)
    return sent


def test_clear_targets_the_one_failed_task(client, monkeypatch):
    sent = _capture(monkeypatch)
    client.clear_task_instance("retail_etl", "run-1", "clean_orders")
    assert sent["url"].endswith("/dags/retail_etl/clearTaskInstances")
    assert sent["body"]["task_ids"] == ["clean_orders"]
    assert sent["body"]["dag_run_id"] == "run-1"


def test_clear_is_not_a_dry_run(client, monkeypatch):
    """Airflow defaults dry_run to true. Leaving it out reruns nothing while
    reporting success, which would look exactly like a working demo."""
    sent = _capture(monkeypatch)
    client.clear_task_instance("retail_etl", "run-1", "clean_orders")
    assert sent["body"]["dry_run"] is False


def test_clear_runs_against_the_patched_code(client, monkeypatch):
    """The point of the rerun is to exercise the new file, not the version the
    run originally used."""
    sent = _capture(monkeypatch)
    client.clear_task_instance("retail_etl", "run-1", "clean_orders")
    assert sent["body"]["run_on_latest_version"] is True


def test_clear_reaches_the_tasks_the_failure_blocked(client, monkeypatch):
    """Downstream tasks are in upstream_failed because of this failure, so a
    fix that leaves them there produces a green task inside a red run."""
    sent = _capture(monkeypatch)
    client.clear_task_instance("retail_etl", "run-1", "clean_orders")
    assert sent["body"]["include_downstream"] is True


def test_clear_does_not_rerun_work_that_already_succeeded(client, monkeypatch):
    """Scoped by task_ids and no upstream, so successful earlier tasks stand."""
    sent = _capture(monkeypatch)
    client.clear_task_instance("retail_etl", "run-1", "clean_orders")
    assert sent["body"]["include_upstream"] is False


def test_clear_does_not_filter_to_failed_only(client, monkeypatch):
    """Blocked downstream tasks sit in upstream_failed, not failed. Filtering
    on failed would leave them stranded and the run red after a good fix."""
    sent = _capture(monkeypatch)
    client.clear_task_instance("retail_etl", "run-1", "clean_orders")
    assert sent["body"]["only_failed"] is False


def test_a_rejected_clear_raises(client, monkeypatch):
    _capture(monkeypatch, MockResponse(status_code=422))
    with pytest.raises(TriggerFailed, match="clean_orders"):
        client.clear_task_instance("retail_etl", "run-1", "clean_orders")
