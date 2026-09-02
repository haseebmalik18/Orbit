"""Plugin endpoints are unauthenticated by default in Airflow.

Orbit's serve source code, diffs, and logs, so every data endpoint must reject
callers without a valid Airflow token. Phase 0 confirmed the unauthenticated
baseline with a bare curl; these tests are what close it.
"""

import pytest
import requests
from fastapi.testclient import TestClient

from orbit.api import deps
from orbit.api.app import orbit_api
from orbit.api.deps import get_repository
from orbit.store.repository import Repository

DATA_ENDPOINTS = [
    "/api/incidents",
    "/api/incidents/inc-1",
    "/api/incidents/inc-1/transcript",
    "/api/incidents/inc-1/checks",
    "/api/stats",
]


class MockResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


@pytest.fixture
def client(tmp_path):
    orbit_api.dependency_overrides[get_repository] = lambda: Repository(
        tmp_path / "orbit.db"
    )
    deps.clear_token_cache()
    yield TestClient(orbit_api)
    orbit_api.dependency_overrides.clear()
    deps.clear_token_cache()


@pytest.mark.parametrize("path", DATA_ENDPOINTS)
def test_every_data_endpoint_rejects_anonymous_callers(client, path):
    assert client.get(path).status_code == 401


@pytest.mark.parametrize("path", DATA_ENDPOINTS)
def test_a_malformed_header_is_rejected(client, path):
    response = client.get(path, headers={"Authorization": "not-a-bearer-token"})
    assert response.status_code == 401


def test_health_stays_open(client):
    """Liveness must work without credentials or it is useless."""
    assert client.get("/api/health").status_code == 200


def test_a_token_airflow_accepts_is_allowed(client, monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: MockResponse(200))
    response = client.get("/api/incidents", headers={"Authorization": "Bearer good"})
    assert response.status_code == 200


def test_a_token_airflow_rejects_is_denied(client, monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: MockResponse(401))
    response = client.get("/api/incidents", headers={"Authorization": "Bearer bad"})
    assert response.status_code == 401


def test_an_unreachable_airflow_denies_rather_than_allows(client, monkeypatch):
    """Fail closed: if we cannot verify, we do not serve."""

    def explode(*args, **kwargs):
        raise requests.ConnectionError("api-server down")

    monkeypatch.setattr(requests, "get", explode)
    response = client.get("/api/incidents", headers={"Authorization": "Bearer x"})
    assert response.status_code == 401


def test_a_session_cookie_airflow_accepts_is_allowed(client, monkeypatch):
    """The panel runs in the browser and sends a cookie, not a bearer token."""
    monkeypatch.setattr(requests, "get", lambda *a, **k: MockResponse(200))
    client.cookies.set("session", "airflow-session-value")
    assert client.get("/api/incidents").status_code == 200


def test_a_session_cookie_airflow_rejects_is_denied(client, monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: MockResponse(401))
    client.cookies.set("session", "stale-session")
    assert client.get("/api/incidents").status_code == 401


def test_cookies_are_forwarded_upstream(client, monkeypatch):
    """Airflow can only judge the session if we actually send it."""
    captured = {}

    def capture(*args, **kwargs):
        captured["cookies"] = kwargs.get("cookies")
        return MockResponse(200)

    monkeypatch.setattr(requests, "get", capture)
    client.cookies.set("session", "abc123")
    client.get("/api/incidents")
    assert captured["cookies"]["session"] == "abc123"


def test_a_verified_token_is_cached(client, monkeypatch):
    """One upstream call per token, not one per request."""
    calls = []

    def counting_get(*args, **kwargs):
        calls.append(1)
        return MockResponse(200)

    monkeypatch.setattr(requests, "get", counting_get)
    for _ in range(4):
        client.get("/api/incidents", headers={"Authorization": "Bearer same"})
    assert len(calls) == 1


def test_distinct_tokens_are_verified_separately(client, monkeypatch):
    calls = []

    def counting_get(*args, **kwargs):
        calls.append(1)
        return MockResponse(200)

    monkeypatch.setattr(requests, "get", counting_get)
    client.get("/api/incidents", headers={"Authorization": "Bearer one"})
    client.get("/api/incidents", headers={"Authorization": "Bearer two"})
    assert len(calls) == 2
