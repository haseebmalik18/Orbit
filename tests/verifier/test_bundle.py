import hashlib
import shutil
from pathlib import Path

import pytest

from orbit.agents import stubs
from orbit.contracts import Edit, Evidence, ProposedPatch
from orbit.verifier.bundle import PatchApplicationError, shadow_bundle


def _checksum(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _patch(old: str, new: str, file: str = "example.py") -> ProposedPatch:
    return ProposedPatch(
        edits=[Edit(file=file, old_string=old, new_string=new)], rationale="r"
    )


@pytest.fixture
def source_root(tmp_path):
    root = tmp_path / "dags"
    root.mkdir()
    (root / "example.py").write_text("VALUE = 1\n")
    return root


def test_shadow_copy_is_isolated_from_source(source_root):
    before = _checksum(source_root)
    with shadow_bundle(source_root, None) as shadow:
        (shadow / "example.py").write_text("VALUE = 999\n")
        assert shadow != source_root
    assert _checksum(source_root) == before


def test_patch_is_applied_in_the_shadow_only(source_root):
    before = _checksum(source_root)
    with shadow_bundle(source_root, _patch("VALUE = 1", "VALUE = 2")) as shadow:
        assert (shadow / "example.py").read_text() == "VALUE = 2\n"
    assert _checksum(source_root) == before


def test_unapplyable_patch_raises(source_root):
    with pytest.raises(PatchApplicationError, match="not found"):
        with shadow_bundle(source_root, _patch("NOPE = 0", "NOPE = 1")):
            pass


def test_patch_producing_unparseable_python_raises(source_root):
    with pytest.raises(PatchApplicationError, match="parse"):
        with shadow_bundle(source_root, _patch("VALUE = 1", "VALUE = (")):
            pass


def test_shadow_is_cleaned_up(source_root):
    with shadow_bundle(source_root, None) as shadow:
        captured = shadow
    assert not captured.exists()


def test_shadow_is_cleaned_up_even_when_patch_fails(source_root):
    leaked = []
    try:
        with shadow_bundle(source_root, _patch("NOPE = 0", "NOPE = 1")) as shadow:
            leaked.append(shadow)
    except PatchApplicationError:
        pass
    assert all(not p.exists() for p in leaked)


def test_real_stub_patch_applies_to_the_real_dags_bundle(tmp_path):
    """End-to-end: the Fixer's actual output against the actual pipeline."""
    source = tmp_path / "dags"
    shutil.copytree(Path("dags"), source)
    evidence = Evidence(
        incident_id="inc-1",
        dag_id="retail_etl",
        task_id="clean_orders",
        run_id="run-bad",
        exception_type="KeyError",
        exception_message="'customer_id'",
        log_tail=[],
        source_path="dags/retail_etl.py",
        source_code="",
        git_diff_since_green=None,
        failing_inputs={},
        regression_cases=[],
    )
    patch = stubs.propose_fix(evidence, stubs.diagnose(evidence))
    before = _checksum(source)
    with shadow_bundle(source, patch) as shadow:
        assert "cust_id" in (shadow / "retail_etl.py").read_text()
    assert _checksum(source) == before
