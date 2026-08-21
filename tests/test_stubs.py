import shutil
from pathlib import Path

from orbit.agents import stubs
from orbit.contracts import CheckResult, Evidence, VerificationReport
from orbit.patch import apply_edits, render_diff


def _evidence(exception_type="KeyError") -> Evidence:
    return Evidence(
        incident_id="inc-1",
        dag_id="retail_etl",
        task_id="clean_orders",
        run_id="run-bad",
        exception_type=exception_type,
        exception_message="'customer_id'",
        log_tail=[],
        source_path="dags/retail_etl.py",
        source_code="",
        git_diff_since_green=None,
        failing_inputs={},
        regression_cases=[],
    )


def test_diagnose_is_deterministic():
    assert stubs.diagnose(_evidence()) == stubs.diagnose(_evidence())


def test_diagnose_maps_key_error_to_schema_drift():
    assert stubs.diagnose(_evidence("KeyError")).category == "schema_drift"


def test_diagnose_maps_type_error():
    assert stubs.diagnose(_evidence("TypeError")).category == "type_error"


def test_diagnose_falls_back_to_unknown():
    assert stubs.diagnose(_evidence("OSError")).category == "unknown"


def test_propose_fix_returns_a_scoped_edit():
    evidence = _evidence()
    patch = stubs.propose_fix(evidence, stubs.diagnose(evidence))
    assert patch.files_touched == ["retail_etl.py"]
    assert len(patch.edits) == 1


def test_stub_patch_actually_applies_to_the_real_dag(tmp_path):
    """A stale stub edit would silently break the verifier later."""
    bundle = tmp_path / "dags"
    shutil.copytree(Path("dags"), bundle)
    evidence = _evidence()
    patch = stubs.propose_fix(evidence, stubs.diagnose(evidence))
    apply_edits(bundle, patch)
    assert "cust_id" in (bundle / "retail_etl.py").read_text()


def test_stub_patch_renders_a_readable_diff(tmp_path):
    bundle = tmp_path / "dags"
    shutil.copytree(Path("dags"), bundle)
    evidence = _evidence()
    patch = stubs.propose_fix(evidence, stubs.diagnose(evidence))
    diff = render_diff(bundle, patch)
    assert diff.startswith("--- a/retail_etl.py")
    assert '-                "customer_id": row["customer_id"],' in diff
    assert "+" in diff and "cust_id" in diff


def test_review_agrees_when_all_checks_passed():
    evidence = _evidence()
    diagnosis = stubs.diagnose(evidence)
    patch = stubs.propose_fix(evidence, diagnosis)
    passing = VerificationReport(
        checks=[
            CheckResult(check=c, status="pass", detail={}, duration_ms=1)
            for c in ("repro", "fix", "regression", "scope")
        ],
        regression_passed=3,
        regression_total=3,
    )
    verdict = stubs.review(diagnosis, patch, passing)
    assert verdict.verdict == "addresses_root_cause"
    assert verdict.disagreements == []


def test_review_disagrees_when_verification_failed():
    evidence = _evidence()
    diagnosis = stubs.diagnose(evidence)
    patch = stubs.propose_fix(evidence, diagnosis)
    failing = VerificationReport(
        checks=[CheckResult(check="fix", status="fail", detail={}, duration_ms=1)],
        regression_passed=0,
        regression_total=3,
    )
    verdict = stubs.review(diagnosis, patch, failing)
    assert verdict.verdict != "addresses_root_cause"
    assert verdict.disagreements
