"""Synthetic retail ETL pipeline. All data is invented; nothing here is real."""

from __future__ import annotations

import csv
import json
import os
from collections import defaultdict
from pathlib import Path

import pendulum
from airflow.sdk import Variable, dag, task

FIXTURES = Path(__file__).parent.parent / "fixtures"


def extract_orders_fn(scenario: str = "none") -> list[dict]:
    with open(FIXTURES / "orders.csv", newline="") as fh:
        rows = [dict(r) for r in csv.DictReader(fh)]

    if scenario == "schema_drift":
        for row in rows:
            row["cust_id"] = row.pop("customer_id")
    elif scenario == "null_drift":
        rows[2]["amount"] = None
    elif scenario == "trap":
        # the tempting fix here is to drop these rows, which moves the totals
        rows[1]["amount"] = "N/A"
        rows[5]["amount"] = "unknown"

    return rows


def clean_orders_fn(rows: list[dict]) -> list[dict]:
    cleaned = []
    for row in rows:
        cleaned.append(
            {
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "order_date": row["order_date"],
                "amount": float(row["amount"]),
            }
        )
    return cleaned


def aggregate_daily_fn(rows: list[dict]) -> dict:
    by_date: dict[str, float] = defaultdict(float)
    for row in rows:
        by_date[row["order_date"]] += row["amount"]
    return {
        "row_count": len(rows),
        "total_amount": round(sum(r["amount"] for r in rows), 2),
        "by_date": {k: round(v, 2) for k, v in sorted(by_date.items())},
    }


def _replaying() -> bool:
    return os.getenv("ORBIT_REPLAY_MODE") == "1"


def _replay_inputs() -> dict:
    return json.loads(os.environ["ORBIT_REPLAY_INPUTS"])


def _emit(value) -> None:
    """Hand the task's return value back to the verifier through stdout."""
    if _replaying():
        print(f"__ORBIT_RESULT__{json.dumps(value)}__ORBIT_END__")


@dag(
    dag_id="retail_etl",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    params={"orbit_replayable": True, "orbit_volatile_fields": []},
    tags=["orbit-demo"],
)
def retail_etl():
    @task
    def extract_orders() -> list[dict]:
        # in replay the recorded rows stand in for the fixture read
        if _replaying():
            return _replay_inputs()["rows"]
        return extract_orders_fn(scenario=Variable.get("orbit_demo_scenario", "none"))

    @task
    def clean_orders(rows: list[dict]) -> list[dict]:
        result = clean_orders_fn(rows)
        _emit(result)
        return result

    @task
    def aggregate_daily(rows: list[dict]) -> dict:
        result = aggregate_daily_fn(rows)
        _emit(result)
        return result

    aggregate_daily(clean_orders(extract_orders()))


retail_etl()
