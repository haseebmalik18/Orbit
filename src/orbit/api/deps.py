from __future__ import annotations

import time

import requests
from fastapi import Header, HTTPException

from orbit.config import settings
from orbit.store.repository import Repository

VERIFY_TIMEOUT_S = 5
CACHE_TTL_S = 300

_verified: dict[str, float] = {}


def clear_token_cache() -> None:
    _verified.clear()


def get_repository() -> Repository:
    return Repository(settings.db_path)


def _token_is_valid(token: str) -> bool:
    """Ask Airflow whether it accepts this token.

    Plugin endpoints carry no auth of their own, and Orbit's serve source code
    and diffs. Verifying upstream means we never invent our own notion of who
    is allowed in. Results are cached so this is one call per token, not one
    per request.
    """
    now = time.monotonic()
    seen = _verified.get(token)
    if seen is not None and now - seen < CACHE_TTL_S:
        return True
    try:
        response = requests.get(
            f"{settings.airflow_base_url}/api/v2/version",
            headers={"Authorization": f"Bearer {token}"},
            timeout=VERIFY_TIMEOUT_S,
        )
        response.raise_for_status()
    except requests.RequestException:
        return False
    _verified[token] = now
    return True


def require_auth(authorization: str | None = Header(default=None)) -> None:
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(status_code=401, detail="bearer token required")
    if not _token_is_valid(token.strip()):
        raise HTTPException(status_code=401, detail="token rejected by Airflow")
