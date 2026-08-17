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
    """

    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def trigger_dag(self, dag_id: str, conf: dict[str, Any]) -> str:
        # logical_date must be present even when null; omitting it is a 422
        body = {"logical_date": None, "conf": conf}
        try:
            response = requests.post(
                f"{self.base_url}/dags/{dag_id}/dagRuns",
                json=body,
                headers=self._headers,
                timeout=TIMEOUT_S,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise TriggerFailed(f"could not trigger {dag_id}: {exc}") from exc
        return response.json()["dag_run_id"]

    def get_xcom(
        self, dag_id: str, run_id: str, task_id: str, key: str = "return_value"
    ) -> Any:
        response = requests.get(
            f"{self.base_url}/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/xcomEntries/{key}",
            headers=self._headers,
            timeout=TIMEOUT_S,
        )
        response.raise_for_status()
        return response.json()["value"]

    def list_successful_runs(self, dag_id: str, task_id: str, limit: int) -> list[str]:
        response = requests.get(
            f"{self.base_url}/dags/{dag_id}/dagRuns",
            params={"state": "success", "limit": limit, "order_by": "-logical_date"},
            headers=self._headers,
            timeout=TIMEOUT_S,
        )
        response.raise_for_status()
        return [r["dag_run_id"] for r in response.json()["dag_runs"]]

    def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int
    ) -> list[str]:
        response = requests.get(
            f"{self.base_url}/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/logs/{try_number}",
            headers={**self._headers, "Accept": "application/json"},
            timeout=TIMEOUT_S,
        )
        response.raise_for_status()
        content = response.json().get("content", "")
        if isinstance(content, list):
            return [str(item) for item in content]
        return str(content).splitlines()
