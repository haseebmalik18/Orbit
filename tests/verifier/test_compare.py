from orbit.verifier.compare import compare


def test_identical_outputs_match():
    assert compare({"row_count": 8}, {"row_count": 8}, [])["match"] is True


def test_differing_value_is_reported():
    result = compare({"row_count": 6}, {"row_count": 8}, [])
    assert result["match"] is False
    assert any("row_count" in d for d in result["differences"])


def test_declared_volatile_field_is_ignored():
    actual = {"row_count": 8, "processed_at": "2026-08-13T10:00:00"}
    expected = {"row_count": 8, "processed_at": "2026-08-12T09:00:00"}
    assert compare(actual, expected, ["processed_at"])["match"] is True


def test_undeclared_nondeterminism_is_a_failure():
    actual = {"row_count": 8, "processed_at": "2026-08-13T10:00:00"}
    expected = {"row_count": 8, "processed_at": "2026-08-12T09:00:00"}
    assert compare(actual, expected, [])["match"] is False


def test_nested_volatile_fields_are_ignored_at_any_depth():
    actual = {"summary": {"total": 10, "generated_at": "a"}}
    expected = {"summary": {"total": 10, "generated_at": "b"}}
    assert compare(actual, expected, ["generated_at"])["match"] is True


def test_volatile_field_inside_a_list_is_ignored():
    actual = {"rows": [{"id": 1, "seen_at": "a"}, {"id": 2, "seen_at": "b"}]}
    expected = {"rows": [{"id": 1, "seen_at": "x"}, {"id": 2, "seen_at": "y"}]}
    assert compare(actual, expected, ["seen_at"])["match"] is True


def test_list_length_change_is_reported():
    result = compare([1, 2], [1, 2, 3], [])
    assert result["match"] is False
    assert any("length" in d for d in result["differences"])


def test_missing_key_is_reported():
    result = compare({"a": 1}, {"a": 1, "b": 2}, [])
    assert result["match"] is False
    assert any("b" in d for d in result["differences"])


def test_unexpected_extra_key_is_reported():
    result = compare({"a": 1, "c": 3}, {"a": 1}, [])
    assert result["match"] is False
    assert any("c" in d for d in result["differences"])


def test_silently_dropped_rows_are_caught():
    """The trap scenario: a plausible fix that quietly changes the totals."""
    expected = {"row_count": 8, "total_amount": 782.58}
    actual = {"row_count": 6, "total_amount": 650.58}
    result = compare(actual, expected, [])
    assert result["match"] is False
    assert len(result["differences"]) == 2


def test_type_change_is_reported():
    result = compare({"amount": "10.0"}, {"amount": 10.0}, [])
    assert result["match"] is False


def test_deeply_nested_difference_is_pathed():
    actual = {"a": {"b": {"c": 1}}}
    expected = {"a": {"b": {"c": 2}}}
    result = compare(actual, expected, [])
    assert any("a.b.c" in d for d in result["differences"])


def test_scalar_outputs_compare():
    assert compare(5, 5, [])["match"] is True
    assert compare(5, 6, [])["match"] is False


def test_none_outputs_compare():
    assert compare(None, None, [])["match"] is True
