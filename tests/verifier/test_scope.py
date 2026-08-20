from orbit.contracts import Diagnosis, ProposedPatch
from orbit.verifier.scope import changed_symbols, check_scope

BEFORE = """
def alpha():
    return 1

def beta():
    return 2
"""

AFTER_ALPHA_ONLY = """
def alpha():
    return 99

def beta():
    return 2
"""

AFTER_BOTH = """
def alpha():
    return 99

def beta():
    return 88
"""

AFTER_ALPHA_REMOVED = """
def beta():
    return 2
"""

AFTER_REFORMATTED = """
def alpha():
    return 1


def beta():
    return 2
"""


def _diagnosis(symbols):
    return Diagnosis(
        root_cause="x",
        category="logic",
        confidence=0.5,
        affected_symbols=symbols,
        reasoning="y",
    )


def _patch(files):
    return ProposedPatch(unified_diff="", files_touched=files, rationale="r")


def _roots(tmp_path, before, after, name="m.py"):
    root = tmp_path / "dags"
    root.mkdir()
    (root / name).write_text(before)
    shadow = tmp_path / "shadow"
    shadow.mkdir()
    (shadow / name).write_text(after)
    return root, shadow


def test_changed_symbols_detects_only_modified_function():
    assert changed_symbols(BEFORE, AFTER_ALPHA_ONLY) == {"alpha"}


def test_changed_symbols_detects_multiple():
    assert changed_symbols(BEFORE, AFTER_BOTH) == {"alpha", "beta"}


def test_changed_symbols_detects_removal():
    assert changed_symbols(BEFORE, AFTER_ALPHA_REMOVED) == {"alpha"}


def test_whitespace_only_change_is_not_a_symbol_change():
    """AST comparison, so reformatting must not read as a behaviour change."""
    assert changed_symbols(BEFORE, AFTER_REFORMATTED) == set()


def test_scope_passes_when_change_matches_diagnosis(tmp_path):
    root, shadow = _roots(tmp_path, BEFORE, AFTER_ALPHA_ONLY)
    result = check_scope(_patch(["m.py"]), _diagnosis(["alpha"]), root, shadow)
    assert result.status == "pass"
    assert result.check == "scope"


def test_scope_fails_when_patch_touches_undeclared_symbol(tmp_path):
    root, shadow = _roots(tmp_path, BEFORE, AFTER_BOTH)
    result = check_scope(_patch(["m.py"]), _diagnosis(["alpha"]), root, shadow)
    assert result.status == "fail"
    assert "beta" in result.detail["undeclared"]


def test_scope_passes_when_declaring_more_than_changed(tmp_path):
    root, shadow = _roots(tmp_path, BEFORE, AFTER_ALPHA_ONLY)
    result = check_scope(_patch(["m.py"]), _diagnosis(["alpha", "beta"]), root, shadow)
    assert result.status == "pass"


def test_scope_reports_declared_and_changed(tmp_path):
    root, shadow = _roots(tmp_path, BEFORE, AFTER_ALPHA_ONLY)
    result = check_scope(_patch(["m.py"]), _diagnosis(["alpha"]), root, shadow)
    assert result.detail["declared"] == ["alpha"]
    assert result.detail["changed"] == ["alpha"]


def test_scope_ignores_files_missing_from_either_side(tmp_path):
    root, shadow = _roots(tmp_path, BEFORE, AFTER_ALPHA_ONLY)
    result = check_scope(_patch(["ghost.py"]), _diagnosis([]), root, shadow)
    assert result.status == "pass"
