from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    id TEXT PRIMARY KEY,
    dag_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    try_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    exception_type TEXT,
    exception_message TEXT,
    resolution TEXT,
    detected_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE TABLE IF NOT EXISTS agent_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id TEXT NOT NULL REFERENCES incidents(id),
    agent TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    model TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS verification_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id TEXT NOT NULL REFERENCES incidents(id),
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id TEXT NOT NULL REFERENCES incidents(id),
    path TEXT NOT NULL,
    human_choice TEXT,
    decided_at TEXT NOT NULL,
    decided_by TEXT
);

CREATE TABLE IF NOT EXISTS token_costs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id TEXT NOT NULL REFERENCES incidents(id),
    agent TEXT NOT NULL,
    model TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    usd REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_incidents_run ON incidents(dag_id, task_id, run_id);
"""


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)
    connection.commit()
