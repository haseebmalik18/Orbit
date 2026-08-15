from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ORBIT_OWN_DAGS = frozenset({"orbit_remediation"})


def _csv_env(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if not raw:
        return list(default)
    return [part.strip() for part in raw.split(",") if part.strip()]


@dataclass
class Settings:
    db_path: Path = field(
        default_factory=lambda: Path(
            os.getenv("ORBIT_DB_PATH", "/usr/local/airflow/orbit.db")
        )
    )
    airflow_api_url: str = field(
        default_factory=lambda: os.getenv(
            "ORBIT_AIRFLOW_API_URL", "http://localhost:8080/api/v2"
        )
    )
    airflow_api_token: str = field(
        default_factory=lambda: os.getenv("ORBIT_AIRFLOW_API_TOKEN", "")
    )
    regression_case_count: int = field(
        default_factory=lambda: int(os.getenv("ORBIT_REGRESSION_CASES", "5"))
    )
    replay_timeout_s: int = field(
        default_factory=lambda: int(os.getenv("ORBIT_REPLAY_TIMEOUT_S", "120"))
    )
    log_tail_lines: int = field(
        default_factory=lambda: int(os.getenv("ORBIT_LOG_TAIL_LINES", "200"))
    )
    watched_dag_ids: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        configured = _csv_env("ORBIT_WATCHED_DAGS", ["retail_etl"])
        # Recursion guard: Orbit must never remediate itself.
        self.watched_dag_ids = [d for d in configured if d not in ORBIT_OWN_DAGS]


settings = Settings()
