from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from orbit.store.schema import initialize

VALID_STATUSES = frozenset(
    {
        "detected",
        "diagnosing",
        "verifying",
        "awaiting_human",
        "applying",
        "resolved",
        "rejected",
        "failed",
        "abandoned",
    }
)
TERMINAL_STATUSES = frozenset({"resolved", "rejected", "failed", "abandoned"})


class InvalidTransition(ValueError):
    """Unknown status value, unknown incident, or already-terminal incident."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            initialize(conn)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def create_incident(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        try_number: int,
        exception_type: str,
        exception_message: str,
    ) -> str:
        incident_id = f"inc-{uuid.uuid4().hex[:12]}"
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO incidents (id, dag_id, task_id, run_id, try_number,"
                " status, exception_type, exception_message, detected_at)"
                " VALUES (?, ?, ?, ?, ?, 'detected', ?, ?, ?)",
                (
                    incident_id,
                    dag_id,
                    task_id,
                    run_id,
                    try_number,
                    exception_type,
                    exception_message,
                    _now(),
                ),
            )
        return incident_id

    def get_incident(self, incident_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM incidents WHERE id = ?", (incident_id,)
            ).fetchone()
        return dict(row) if row else None

    def set_status(self, incident_id: str, status: str) -> None:
        if status not in VALID_STATUSES:
            raise InvalidTransition(f"unknown status: {status}")
        current = self.get_incident(incident_id)
        if current is None:
            raise InvalidTransition(f"unknown incident: {incident_id}")
        if current["status"] in TERMINAL_STATUSES:
            raise InvalidTransition(
                f"incident {incident_id} is terminal ({current['status']})"
            )
        resolved_at = _now() if status in TERMINAL_STATUSES else None
        with self._connect() as conn:
            conn.execute(
                "UPDATE incidents SET status = ?,"
                " resolved_at = COALESCE(?, resolved_at) WHERE id = ?",
                (status, resolved_at, incident_id),
            )

    def set_resolution(self, incident_id: str, resolution: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE incidents SET resolution = ? WHERE id = ?",
                (resolution, incident_id),
            )

    def list_incidents(
        self,
        dag_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        clauses, params = [], []
        for column, value in (
            ("dag_id", dag_id),
            ("task_id", task_id),
            ("run_id", run_id),
            ("status", status),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM incidents{where} ORDER BY detected_at DESC", params
            ).fetchall()
        return [dict(r) for r in rows]

    def add_message(
        self,
        incident_id: str,
        agent: str,
        role: str,
        content: str,
        model: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO agent_messages (incident_id, agent, role, content,"
                " model, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (incident_id, agent, role, content, model, _now()),
            )

    def get_messages(self, incident_id: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM agent_messages WHERE incident_id = ? ORDER BY id",
                (incident_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def add_check(
        self,
        incident_id: str,
        check: str,
        status: str,
        detail: dict[str, Any],
        duration_ms: int,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO verification_checks (incident_id, check_name, status,"
                " detail_json, duration_ms, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (incident_id, check, status, json.dumps(detail), duration_ms, _now()),
            )

    def get_checks(self, incident_id: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM verification_checks WHERE incident_id = ? ORDER BY id",
                (incident_id,),
            ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["check"] = item.pop("check_name")
            item["detail"] = json.loads(item.pop("detail_json"))
            out.append(item)
        return out

    def record_decision(
        self, incident_id: str, path: str, human_choice: str, decided_by: str
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO decisions (incident_id, path, human_choice, decided_at,"
                " decided_by) VALUES (?, ?, ?, ?, ?)",
                (incident_id, path, human_choice, _now(), decided_by),
            )

    def add_cost(
        self,
        incident_id: str,
        agent: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        usd: float,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO token_costs (incident_id, agent, model, input_tokens,"
                " output_tokens, usd) VALUES (?, ?, ?, ?, ?, ?)",
                (incident_id, agent, model, input_tokens, output_tokens, usd),
            )

    def stats(self) -> dict:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT resolution, COUNT(*) AS n FROM incidents"
                " WHERE resolution IS NOT NULL GROUP BY resolution"
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) AS n FROM incidents").fetchone()["n"]
            cost = conn.execute(
                "SELECT COALESCE(SUM(usd), 0) AS usd FROM token_costs"
            ).fetchone()["usd"]
        counts = {r["resolution"]: r["n"] for r in rows}
        return {
            "total_incidents": total,
            "verified": counts.get("verified_awaiting_approval", 0),
            "escalated": counts.get("escalated_not_verified", 0),
            "total_usd": round(cost, 4),
        }
