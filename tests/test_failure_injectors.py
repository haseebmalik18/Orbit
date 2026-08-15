import pytest

from dags.retail_etl import aggregate_daily_fn, clean_orders_fn, extract_orders_fn


def _parses_as_float(value) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def test_schema_drift_raises_key_error():
    rows = extract_orders_fn(scenario="schema_drift")
    assert "cust_id" in rows[0] and "customer_id" not in rows[0]
    with pytest.raises(KeyError):
        clean_orders_fn(rows)


def test_null_drift_raises_type_error():
    rows = extract_orders_fn(scenario="null_drift")
    assert any(r["amount"] is None for r in rows)
    with pytest.raises(TypeError):
        clean_orders_fn(rows)


def test_trap_raises_value_error():
    rows = extract_orders_fn(scenario="trap")
    with pytest.raises(ValueError):
        clean_orders_fn(rows)


def test_trap_naive_fix_silently_changes_totals():
    healthy = aggregate_daily_fn(clean_orders_fn(extract_orders_fn("none")))
    rows = extract_orders_fn(scenario="trap")
    naive = [r for r in rows if _parses_as_float(r["amount"])]
    dropped = aggregate_daily_fn(clean_orders_fn(naive))
    assert dropped["row_count"] < healthy["row_count"]
    assert dropped["total_amount"] != healthy["total_amount"]


def test_none_scenario_is_unchanged():
    assert extract_orders_fn(scenario="none") == extract_orders_fn()
