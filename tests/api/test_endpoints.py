import pytest
from fastapi.testclient import TestClient

from orbit.api.app import orbit_api
from orbit.api.deps import get_repository, require_auth
from orbit.store.repository import Repository


@pytest.fixture
def repo(tmp_path):
    return Repository(tmp_path / "orbit.db")


@pytest.fixture
def client(repo):
    """Auth is exercised separately in test_auth.py."""
    orbit_api.dependency_overrides[get_repository] = lambda: repo
    orbit_api.dependency_overrides[require_auth] = lambda: None
    yield TestClient(orbit_api)
    orbit_api.dependency_overrides.clear()


@pytest.fixture
def incident(repo):
    incident_id = repo.create_incident(
        "retail_etl", "clean_orders", "run-1", 1, "KeyError", "'customer_id'"
    )
    repo.add_message(incident_id, "detector", "attempt", "calling detector")
    repo.add_message(incident_id, "detector", "assistant", "schema drift", "groq:x")
    repo.add_message(incident_id, "reviewer", "assistant", "looks right", "mistral:y")
    repo.add_check(incident_id, "repro", "pass", {"observed": "KeyError"}, 4100)
    repo.add_check(incident_id, "regression", "fail", {"passed": 0, "total": 5}, 20000)
    return incident_id


def test_health_needs_no_auth():
    assert TestClient(orbit_api).get("/api/health").status_code == 200


def test_list_incidents(client, incident):
    body = client.get("/api/incidents").json()
    assert len(body["incidents"]) == 1
    assert body["incidents"][0]["id"] == incident


def test_list_incidents_filters_by_run(client, incident):
    assert client.get("/api/incidents?run_id=run-1").json()["incidents"]
    assert not client.get("/api/incidents?run_id=other").json()["incidents"]


def test_list_incidents_filters_by_task(client, incident):
    """The task-destination panel filters to the task being viewed."""
    assert client.get("/api/incidents?task_id=clean_orders").json()["incidents"]
    assert not client.get("/api/incidents?task_id=aggregate_daily").json()["incidents"]


def test_incident_detail(client, incident):
    body = client.get(f"/api/incidents/{incident}").json()
    assert body["id"] == incident
    assert body["exception_type"] == "KeyError"


def test_unknown_incident_is_404(client):
    assert client.get("/api/incidents/inc-nope").status_code == 404


def test_transcript_hides_bookkeeping_attempts(client, incident):
    """`attempt` rows are budget accounting, not something a human reads."""
    messages = client.get(f"/api/incidents/{incident}/transcript").json()["messages"]
    assert [m["agent"] for m in messages] == ["detector", "reviewer"]
    assert all(m["role"] == "assistant" for m in messages)


def test_transcript_carries_the_model(client, incident):
    messages = client.get(f"/api/incidents/{incident}/transcript").json()["messages"]
    assert messages[0]["model"] == "groq:x"
    assert messages[1]["model"] == "mistral:y"


def test_checks_include_detail_and_duration(client, incident):
    checks = client.get(f"/api/incidents/{incident}/checks").json()["checks"]
    assert {c["check"] for c in checks} == {"repro", "regression"}
    failed = next(c for c in checks if c["check"] == "regression")
    assert failed["detail"]["total"] == 5
    assert failed["duration_ms"] == 20000


def test_checks_report_progress_for_the_gauge(client, incident):
    """The gauge needs to know how far through verification we are."""
    body = client.get(f"/api/incidents/{incident}/checks").json()
    assert body["completed"] == 2
    assert body["total"] == 4
    assert body["passed"] == 1


def test_stats_counts_outcomes(client, repo, incident):
    """Off the resolution the pipeline writes, not off a human decision — the
    panel has to show real counts before anyone has approved anything."""
    other = repo.create_incident("retail_etl", "clean_orders", "run-2", 1, "E", "m")
    repo.set_resolution(incident, "verified_awaiting_approval")
    repo.set_resolution(other, "escalated_not_verified")
    body = client.get("/api/stats").json()
    assert body["verified"] == 1
    assert body["escalated"] == 1
    assert body["total_incidents"] == 2


def test_transcript_of_unknown_incident_is_404(client):
    assert client.get("/api/incidents/inc-nope/transcript").status_code == 404


def test_checks_of_unknown_incident_is_404(client):
    assert client.get("/api/incidents/inc-nope/checks").status_code == 404
