from __future__ import annotations

from fastapi import FastAPI

from orbit import __version__

orbit_api = FastAPI(title="Orbit API")


@orbit_api.get("/api/health")
def health() -> dict:
    return {"status": "ok", "version": __version__}
