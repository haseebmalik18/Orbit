from __future__ import annotations

from typing import Any

import requests

TIMEOUT_S = 10


class TriggerFailed(RuntimeError):
    """The Airflow REST API rejected a request or was unreachable."""


class AirflowClient:
    """Thin REST API v2 client.

    Airflow 3 removed metadata-DB access from worker and task context, so every
    read of Airflow state goes through here rather than a SQLAlchemy session.

    Pass `token` to use a fixed credential, otherwise one is minted from
    /auth/token on first use. SimpleAuthManager tokens expire after 24h, so
    minting beats baking a static token into the environment.
    """

    def __init__(
        self,
        base_url: str,
        token: str = "",
        username: str = "admin",
        password: str = "admin",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self._token = token or ""

    @property
    def api_url(self) -> str:
        return f"{self.base_url}/api/v2"

    def _mint_token(self) -> str:
        try:
            response = requests.post(
                f"{self.base_url}/auth/token",
                json={"username": self.username, "password": self.password},
                headers={"Content-Type": "application/json"},
                timeout=TIMEOUT_S,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise TriggerFailed(f"could not obtain an Airflow token: {exc}") from exc
        return response.json()["access_token"]

    def _headers(self) -> dict[str, str]:
        if not self._token:
            self._token = self._mint_token()
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def trigger_dag(self, dag_id: str, conf: dict[str, Any]) -> str:
        headers = self._headers()
        # logical_date must be present even when null; omitting it is a 422
        body = {"logical_date": None, "conf": conf}
        try:
            response = requests.post(
                f"{self.api_url}/dags/{dag_id}/dagRuns",
                json=body,
                headers=headers,
                timeout=TIMEOUT_S,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise TriggerFailed(f"could not trigger {dag_id}: {exc}") from exc
        return response.json()["dag_run_id"]

    def clear_task_instance(self, dag_id: str, run_id: str, task_id: str) -> None:
        """Rerun one failed task in place, against the patched code.

        Clearing rather than triggering a fresh run is deliberate: it turns the
        original failure green instead of leaving it on screen beside a
        separate success.
        """
        body = {
            "dag_run_id": run_id,
            "task_ids": [task_id],
            # Airflow defaults this to true; omitting it clears nothing and
            # still returns 200
            "dry_run": False,
            "only_failed": True,
            "include_upstream": False,
            "include_downstream": False,
            "include_past": False,
            "include_future": False,
            "reset_dag_runs": True,
            "run_on_latest_version": True,
        }
        try:
            response = requests.post(
                f"{self.api_url}/dags/{dag_id}/clearTaskInstances",
                json=body,
                headers=self._headers(),
                timeout=TIMEOUT_S,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise TriggerFailed(
                f"could not rerun {dag_id}.{task_id} in {run_id}: {exc}"
            ) from exc

    def get_xcom(
        self, dag_id: str, run_id: str, task_id: str, key: str = "return_value"
    ) -> Any:
        response = requests.get(
            f"{self.api_url}/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/xcomEntries/{key}",
            headers=self._headers(),
            timeout=TIMEOUT_S,
        )
        response.raise_for_status()
        return response.json()["value"]

    def list_successful_runs(self, dag_id: str, task_id: str, limit: int) -> list[str]:
        response = requests.get(
            f"{self.api_url}/dags/{dag_id}/dagRuns",
            params={"state": "success", "limit": limit, "order_by": "-logical_date"},
            headers=self._headers(),
            timeout=TIMEOUT_S,
        )
        response.raise_for_status()
        return [r["dag_run_id"] for r in response.json()["dag_runs"]]

    def get_dag_params(self, dag_id: str) -> dict[str, Any]:
        """Read a DAG's declared params.

        Airflow wraps each one as {value, schema, description}; older shapes
        return the bare value, so handle both.
        """
        response = requests.get(
            f"{self.api_url}/dags/{dag_id}/details",
            headers=self._headers(),
            timeout=TIMEOUT_S,
        )
        response.raise_for_status()
        params = response.json().get("params") or {}
        return {
            name: spec["value"] if isinstance(spec, dict) and "value" in spec else spec
            for name, spec in params.items()
        }

    def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int
    ) -> list[str]:
        response = requests.get(
            f"{self.api_url}/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/logs/{try_number}",
            headers={**self._headers(), "Accept": "application/json"},
            timeout=TIMEOUT_S,
        )
        response.raise_for_status()
        content = response.json().get("content", "")
        if isinstance(content, list):
            return [str(item) for item in content]
        return str(content).splitlines()
