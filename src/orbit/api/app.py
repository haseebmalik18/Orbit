from __future__ import annotations

from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse

from orbit import __version__
from orbit.api.deps import get_repository, require_auth
from orbit.contracts import REQUIRED_CHECKS
from orbit.store.repository import Repository

PANEL = Path(__file__).parent / "static" / "panel.html"

orbit_api = FastAPI(title="Orbit API")

# every data route requires a token; /api/health deliberately does not
protected = [Depends(require_auth)]


@orbit_api.get("/api/health")
def health() -> dict:
    return {"status": "ok", "version": __version__}


@orbit_api.get("/api/incidents", dependencies=protected)
def list_incidents(
    dag_id: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
    status: str | None = None,
    repo: Repository = Depends(get_repository),
) -> dict:
    return {
        "incidents": repo.list_incidents(
            dag_id=dag_id, task_id=task_id, run_id=run_id, status=status
        )
    }


def _incident_or_404(repo: Repository, incident_id: str) -> dict:
    incident = repo.get_incident(incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail=f"unknown incident {incident_id}")
    return incident


@orbit_api.get("/api/incidents/{incident_id}", dependencies=protected)
def get_incident(
    incident_id: str, repo: Repository = Depends(get_repository)
) -> dict:
    return _incident_or_404(repo, incident_id)


@orbit_api.get("/api/incidents/{incident_id}/transcript", dependencies=protected)
def get_transcript(
    incident_id: str, repo: Repository = Depends(get_repository)
) -> dict:
    _incident_or_404(repo, incident_id)
    messages = [
        m for m in repo.get_messages(incident_id) if m["role"] == "assistant"
    ]
    return {"messages": messages}


@orbit_api.get("/api/incidents/{incident_id}/checks", dependencies=protected)
def get_checks(incident_id: str, repo: Repository = Depends(get_repository)) -> dict:
    _incident_or_404(repo, incident_id)
    checks = repo.get_checks(incident_id)
    return {
        "checks": checks,
        "completed": len(checks),
        "total": len(REQUIRED_CHECKS),
        "passed": sum(1 for c in checks if c["status"] == "pass"),
    }


@orbit_api.get("/api/stats", dependencies=protected)
def get_stats(repo: Repository = Depends(get_repository)) -> dict:
    return repo.stats()


@orbit_api.get("/ui/", include_in_schema=False)
def panel() -> FileResponse:
    """The panel shell. Its data calls hit /api/* and carry the caller's
    Airflow session, so the page itself needs no credentials of its own."""
    return FileResponse(PANEL, media_type="text/html")
