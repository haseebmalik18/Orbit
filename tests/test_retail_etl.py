from dags.retail_etl import aggregate_daily_fn, clean_orders_fn, extract_orders_fn


def test_extract_returns_json_serializable_records():
    rows = extract_orders_fn(scenario="none")
    assert isinstance(rows, list)
    assert all(isinstance(r, dict) for r in rows)
    assert rows[0]["customer_id"] == "C001"
    assert len(rows) == 8


def test_clean_drops_nothing_on_healthy_input():
    rows = extract_orders_fn(scenario="none")
    cleaned = clean_orders_fn(rows)
    assert len(cleaned) == len(rows)
    assert all(isinstance(r["amount"], float) for r in cleaned)


def test_aggregate_produces_stable_totals():
    rows = clean_orders_fn(extract_orders_fn(scenario="none"))
    result = aggregate_daily_fn(rows)
    assert result["row_count"] == 8
    assert round(result["total_amount"], 2) == 782.58
    assert set(result["by_date"]) == {
        "2026-08-01",
        "2026-08-02",
        "2026-08-03",
        "2026-08-04",
    }
