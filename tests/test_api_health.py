from fastapi.testclient import TestClient

from orbit.api.app import orbit_api


def test_health_returns_ok():
    client = TestClient(orbit_api)
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "0.1.0"}
