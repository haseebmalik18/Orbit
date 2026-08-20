from __future__ import annotations

from typing import Any


def _strip(value: Any, volatile: set[str]) -> Any:
    if isinstance(value, dict):
        return {k: _strip(v, volatile) for k, v in value.items() if k not in volatile}
    if isinstance(value, list):
        return [_strip(item, volatile) for item in value]
    return value


def _diff(actual: Any, expected: Any, path: str, out: list[str]) -> None:
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in sorted(set(expected) | set(actual)):
            if key not in actual:
                out.append(f"{path}.{key}: missing from actual")
            elif key not in expected:
                out.append(f"{path}.{key}: unexpected in actual")
            else:
                _diff(actual[key], expected[key], f"{path}.{key}", out)
        return
    if isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            out.append(f"{path}: length {len(actual)} != expected {len(expected)}")
            return
        for index, (a, e) in enumerate(zip(actual, expected)):
            _diff(a, e, f"{path}[{index}]", out)
        return
    if type(actual) is not type(expected) or actual != expected:
        out.append(f"{path}: {actual!r} != expected {expected!r}")


def compare(actual: Any, expected: Any, volatile_fields: list[str]) -> dict:
    """Structurally compare a replay result against a recorded output.

    Anything not declared volatile must match exactly. Undeclared
    nondeterminism surfacing as a failure is correct, not a bug.
    """
    volatile = set(volatile_fields)
    differences: list[str] = []
    _diff(_strip(actual, volatile), _strip(expected, volatile), "root", differences)
    return {"match": not differences, "differences": differences}
