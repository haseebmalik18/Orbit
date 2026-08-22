from contextlib import nullcontext
from pathlib import Path

from orbit.contracts import Case, CheckResult, Diagnosis, Edit, Evidence, ProposedPatch
from orbit.verifier import runner
from orbit.verifier.bundle import PatchApplicationError
from orbit.verifier.replay import ReplayResult

SOURCE = Path("dags")


def _evidence(cases=None, replayable=True, volatile=None) -> Evidence:
    return Evidence(
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
        failing_inputs={"rows": [{"cust_id": "C001"}]},
        regression_cases=cases if cases is not None else [],
        replayable=replayable,
        volatile_fields=volatile or [],
    )


def _patch() -> ProposedPatch:
    return ProposedPatch(
        edits=[Edit(file="retail_etl.py", old_string="x", new_string="y")],
        rationale="r",
    )


def _diagnosis() -> Diagnosis:
    return Diagnosis(
        root_cause="x",
        category="schema_drift",
        confidence=0.9,
        affected_symbols=[],
        reasoning="y",
    )


def _ok(output=None):
    return ReplayResult(True, None, output, "", "", 10, False)


def _fail(exception_type="KeyError"):
    return ReplayResult(False, exception_type, None, "", "", 10, False)


def _case(run_id="r1", expected=None):
    return Case(
        run_id=run_id,
        inputs={"rows": []},
        expected_output=expected if expected is not None else {"row_count": 8},
    )


def _install(monkeypatch, results, scope_status="pass", bundle_raises=False):
    queue = list(results)
    monkeypatch.setattr(runner, "replay", lambda *a, **k: queue.pop(0))

    def mock_bundle(source_root, patch):
        if bundle_raises and patch is not None:
            raise PatchApplicationError("patch did not apply")
        return nullcontext(Path("/tmp/shadow"))

    monkeypatch.setattr(runner, "shadow_bundle", mock_bundle)
    monkeypatch.setattr(
        runner,
        "check_scope",
        lambda *a, **k: CheckResult(
            check="scope", status=scope_status, detail={}, duration_ms=1
        ),
    )


def _check(report, name):
    return next(c for c in report.checks if c.check == name)


def test_all_checks_pass_on_a_good_patch(monkeypatch):
    _install(monkeypatch, [_fail(), _ok(), _ok({"row_count": 8})])
    report = runner.verify(_evidence([_case()]), _patch(), _diagnosis(), SOURCE)
    assert report.all_passed is True
    assert report.regression_passed == 1
    assert report.regression_total == 1


def test_repro_failure_stops_verification(monkeypatch):
    """If the bug does not reproduce, nothing downstream can be trusted."""
    _install(monkeypatch, [_ok()])
    report = runner.verify(_evidence(), _patch(), _diagnosis(), SOURCE)
    assert report.all_passed is False
    assert _check(report, "repro").status == "fail"
    assert _check(report, "fix").status == "skipped"
    assert _check(report, "regression").status == "skipped"
    assert _check(report, "scope").status == "skipped"


def test_regression_catches_a_behaviour_changing_patch(monkeypatch):
    """The check the whole project rests on."""
    _install(monkeypatch, [_fail(), _ok(), _ok({"row_count": 6})])
    report = runner.verify(_evidence([_case()]), _patch(), _diagnosis(), SOURCE)
    assert report.all_passed is False
    assert _check(report, "regression").status == "fail"
    assert report.regression_passed == 0


def test_regression_counts_partial_passes(monkeypatch):
    cases = [_case("r1"), _case("r2"), _case("r3")]
    _install(
        monkeypatch,
        [_fail(), _ok(), _ok({"row_count": 8}), _ok({"row_count": 6}), _ok({"row_count": 8})],
    )
    report = runner.verify(_evidence(cases), _patch(), _diagnosis(), SOURCE)
    assert report.regression_passed == 2
    assert report.regression_total == 3
    assert _check(report, "regression").status == "fail"


def test_regression_case_that_errors_counts_as_failure(monkeypatch):
    _install(monkeypatch, [_fail(), _ok(), _fail("TypeError")])
    report = runner.verify(_evidence([_case()]), _patch(), _diagnosis(), SOURCE)
    assert report.regression_passed == 0
    assert _check(report, "regression").status == "fail"


def test_no_regression_history_is_skipped_not_passed(monkeypatch):
    _install(monkeypatch, [_fail(), _ok()])
    report = runner.verify(_evidence([]), _patch(), _diagnosis(), SOURCE)
    assert _check(report, "regression").status == "skipped"
    assert report.all_passed is False


def test_fix_failure_fails_the_report(monkeypatch):
    _install(monkeypatch, [_fail(), _fail(), _ok({"row_count": 8})])
    report = runner.verify(_evidence([_case()]), _patch(), _diagnosis(), SOURCE)
    assert _check(report, "fix").status == "fail"
    assert report.all_passed is False


def test_scope_failure_fails_the_report(monkeypatch):
    _install(monkeypatch, [_fail(), _ok(), _ok({"row_count": 8})], scope_status="fail")
    report = runner.verify(_evidence([_case()]), _patch(), _diagnosis(), SOURCE)
    assert report.all_passed is False


def test_unapplyable_patch_fails_rather_than_raising(monkeypatch):
    _install(monkeypatch, [_fail()], bundle_raises=True)
    report = runner.verify(_evidence([_case()]), _patch(), _diagnosis(), SOURCE)
    assert report.all_passed is False
    assert _check(report, "fix").status == "fail"
    assert "did not apply" in str(_check(report, "fix").detail)


def test_volatile_fields_are_honoured_in_regression(monkeypatch):
    case = _case(expected={"row_count": 8, "at": "old"})
    _install(monkeypatch, [_fail(), _ok(), _ok({"row_count": 8, "at": "new"})])
    report = runner.verify(
        _evidence([case]), _patch(), _diagnosis(), SOURCE, volatile_fields=["at"]
    )
    assert _check(report, "regression").status == "pass"
    assert report.all_passed is True


def test_a_task_that_did_not_opt_in_is_never_verified(monkeypatch):
    """Skipping is not passing — this must escalate, not silently approve."""
    _install(monkeypatch, [])
    report = runner.verify(
        _evidence([_case()], replayable=False), _patch(), _diagnosis(), SOURCE
    )
    assert report.all_passed is False
    assert all(c.status == "skipped" for c in report.checks)
    assert "orbit_replayable" in str(_check(report, "repro").detail)


def test_opt_out_makes_no_replay_calls(monkeypatch):
    """Not opting in should cost nothing — no subprocesses, no bundles."""
    calls = []
    monkeypatch.setattr(runner, "replay", lambda *a, **k: calls.append(1))
    runner.verify(_evidence(replayable=False), _patch(), _diagnosis(), SOURCE)
    assert calls == []


def test_volatile_fields_come_from_evidence_when_not_overridden(monkeypatch):
    case = _case(expected={"row_count": 8, "at": "old"})
    _install(monkeypatch, [_fail(), _ok(), _ok({"row_count": 8, "at": "new"})])
    report = runner.verify(
        _evidence([case], volatile=["at"]), _patch(), _diagnosis(), SOURCE
    )
    assert _check(report, "regression").status == "pass"


def test_every_check_is_always_present(monkeypatch):
    _install(monkeypatch, [_fail(), _ok(), _ok({"row_count": 8})])
    report = runner.verify(_evidence([_case()]), _patch(), _diagnosis(), SOURCE)
    assert {c.check for c in report.checks} == {"repro", "fix", "regression", "scope"}
