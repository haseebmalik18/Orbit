import json

from orbit.contracts import (
    Case,
    CheckResult,
    Diagnosis,
    Evidence,
    ProposedPatch,
    ReviewVerdict,
    VerificationReport,
)


def _evidence() -> Evidence:
    return Evidence(
        incident_id="inc-1",
        dag_id="retail_etl",
        task_id="clean_orders",
        run_id="manual__2026-08-13T00:00:00+00:00",
        exception_type="KeyError",
        exception_message="'customer_id'",
        log_tail=["line one", "line two"],
        source_path="dags/retail_etl.py",
        source_code="def clean_orders_fn(rows): ...",
        git_diff_since_green=None,
        failing_inputs={"rows": [{"cust_id": "C001"}]},
        regression_cases=[
            Case(
                run_id="manual__2026-08-12T00:00:00+00:00",
                inputs={"rows": [{"customer_id": "C001"}]},
                expected_output={"row_count": 8},
            )
        ],
    )


def test_evidence_roundtrips_through_json():
    original = _evidence()
    restored = Evidence.model_validate(json.loads(original.model_dump_json()))
    assert restored == original


def test_verification_report_all_passed_is_derived():
    passing = [
        CheckResult(check=c, status="pass", detail={}, duration_ms=1)
        for c in ("repro", "fix", "regression", "scope")
    ]
    report = VerificationReport(checks=passing, regression_passed=3, regression_total=3)
    assert report.all_passed is True


def test_skipped_check_is_not_treated_as_passed():
    checks = [
        CheckResult(check="repro", status="skipped", detail={}, duration_ms=0),
        CheckResult(check="fix", status="skipped", detail={}, duration_ms=0),
        CheckResult(check="regression", status="skipped", detail={}, duration_ms=0),
        CheckResult(check="scope", status="pass", detail={}, duration_ms=1),
    ]
    report = VerificationReport(checks=checks, regression_passed=0, regression_total=0)
    assert report.all_passed is False


def test_missing_check_is_not_treated_as_passed():
    only_scope = [CheckResult(check="scope", status="pass", detail={}, duration_ms=1)]
    report = VerificationReport(
        checks=only_scope, regression_passed=0, regression_total=0
    )
    assert report.all_passed is False


def test_verification_report_survives_xcom_roundtrip():
    passing = [
        CheckResult(check=c, status="pass", detail={"n": 1}, duration_ms=1)
        for c in ("repro", "fix", "regression", "scope")
    ]
    original = VerificationReport(
        checks=passing, regression_passed=3, regression_total=3
    )
    restored = VerificationReport.model_validate(original.model_dump())
    assert restored.all_passed is True
    assert restored.regression_total == 3


def test_contracts_roundtrip():
    for model in (
        Diagnosis(
            root_cause="upstream renamed customer_id",
            category="schema_drift",
            confidence=0.9,
            affected_symbols=["clean_orders_fn"],
            reasoning="traceback shows KeyError",
        ),
        ProposedPatch(
            unified_diff="--- a\n+++ b\n",
            files_touched=["dags/retail_etl.py"],
            rationale="map the renamed column",
        ),
        ReviewVerdict(
            verdict="addresses_root_cause", reasoning="handles rename", disagreements=[]
        ),
    ):
        assert type(model).model_validate(json.loads(model.model_dump_json())) == model
