from __future__ import annotations

import time

import requests
from fastapi import HTTPException, Request

from orbit.config import settings
from orbit.store.repository import Repository

VERIFY_TIMEOUT_S = 5
CACHE_TTL_S = 300

_verified: dict[str, float] = {}


def clear_token_cache() -> None:
    _verified.clear()


def get_repository() -> Repository:
    return Repository(settings.db_path)


def _credentials(request: Request) -> tuple[str, dict, dict] | None:
    """Whatever the caller authenticated with, in a form we can forward.

    The panel runs in the browser and sends Airflow's session cookie; scripts
    and the React panel send a bearer token. Both are valid ways to be logged
    in, so accept either and let Airflow decide.
    """
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() == "bearer" and token.strip():
        return f"bearer:{token.strip()}", {"Authorization": authorization}, {}

    cookies = dict(request.cookies)
    if cookies:
        key = "cookie:" + ";".join(f"{k}={v}" for k, v in sorted(cookies.items()))
        return key, {}, cookies

    return None


def _airflow_accepts(cache_key: str, headers: dict, cookies: dict) -> bool:
    """Ask Airflow whether these credentials are real.

    Verified against /api/v2/dags rather than /api/v2/version: the version
    endpoint is public, so it accepts anything and proves nothing.

    Plugin endpoints carry no auth of their own, and Orbit's serve source code
    and diffs. Verifying upstream means we never invent our own notion of who
    is allowed in. Results are cached so this is one call per credential, not
    one per request.
    """
    now = time.monotonic()
    seen = _verified.get(cache_key)
    if seen is not None and now - seen < CACHE_TTL_S:
        return True
    try:
        response = requests.get(
            f"{settings.airflow_base_url}/api/v2/dags",
            params={"limit": 1},
            headers=headers,
            cookies=cookies,
            timeout=VERIFY_TIMEOUT_S,
        )
        response.raise_for_status()
    except requests.RequestException:
        return False
    _verified[cache_key] = now
    return True


def require_auth(request: Request) -> None:
    credentials = _credentials(request)
    if credentials is None:
        raise HTTPException(status_code=401, detail="authentication required")
    cache_key, headers, cookies = credentials
    if not _airflow_accepts(cache_key, headers, cookies):
        raise HTTPException(status_code=401, detail="credentials rejected by Airflow")
