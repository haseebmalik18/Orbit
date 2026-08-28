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
            os.getenv("ORBIT_DB_PATH", "/usr/local/airflow/.orbit/orbit.db")
        )
    )
    # container-internal hostname: the listener runs in the scheduler, not on the host
    airflow_base_url: str = field(
        default_factory=lambda: os.getenv(
            "ORBIT_AIRFLOW_BASE_URL", "http://api-server:8080"
        )
    )
    # SimpleAuthManager tokens expire in 24h, so we mint them on demand rather
    # than baking a static one into the environment
    airflow_username: str = field(
        default_factory=lambda: os.getenv("ORBIT_AIRFLOW_USERNAME", "admin")
    )
    airflow_password: str = field(
        default_factory=lambda: os.getenv("ORBIT_AIRFLOW_PASSWORD", "admin")
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
    # stubs stay the default so CI and non-agent work cost nothing
    use_stub_agents: bool = field(
        default_factory=lambda: os.getenv("ORBIT_USE_STUB_AGENTS", "1") == "1"
    )
    detector_conn_id: str = field(
        default_factory=lambda: os.getenv("ORBIT_DETECTOR_CONN", "orbit_groq")
    )
    fixer_conn_id: str = field(
        default_factory=lambda: os.getenv("ORBIT_FIXER_CONN", "orbit_groq")
    )
    reviewer_conn_id: str = field(
        default_factory=lambda: os.getenv("ORBIT_REVIEWER_CONN", "orbit_mistral")
    )
    detector_model: str = field(
        default_factory=lambda: os.getenv(
            "ORBIT_DETECTOR_MODEL", "groq:openai/gpt-oss-120b"
        )
    )
    fixer_model: str = field(
        default_factory=lambda: os.getenv(
            "ORBIT_FIXER_MODEL", "groq:openai/gpt-oss-120b"
        )
    )
    # deliberately a different provider: a reviewer sharing the fixer's model
    # shares its blind spots
    reviewer_model: str = field(
        default_factory=lambda: os.getenv(
            "ORBIT_REVIEWER_MODEL", "mistral:mistral-large-latest"
        )
    )
    max_agent_calls_per_incident: int = field(
        default_factory=lambda: int(os.getenv("ORBIT_MAX_AGENT_CALLS", "9"))
    )
    max_prompt_chars: int = field(
        default_factory=lambda: int(os.getenv("ORBIT_MAX_PROMPT_CHARS", "12000"))
    )
    watched_dag_ids: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        configured = _csv_env("ORBIT_WATCHED_DAGS", ["retail_etl"])
        # Recursion guard: Orbit must never remediate itself.
        self.watched_dag_ids = [d for d in configured if d not in ORBIT_OWN_DAGS]


settings = Settings()
