from orbit.agents.prompts import (
    DETECTOR_SYSTEM,
    FIXER_SYSTEM,
    REVIEWER_SYSTEM,
    detector_prompt,
    fixer_prompt,
    reviewer_prompt,
)
from orbit.contracts import (
    CheckResult,
    Diagnosis,
    Edit,
    Evidence,
    ProposedPatch,
    VerificationReport,
)


def _evidence(source="def clean_orders_fn(rows):\n    pass\n", log=None) -> Evidence:
    return Evidence(
        incident_id="inc-1",
        dag_id="retail_etl",
        task_id="clean_orders",
        run_id="run-bad",
        exception_type="KeyError",
        exception_message="'customer_id'",
        log_tail=log if log is not None else ["line one", "KeyError: 'customer_id'"],
        source_path="dags/retail_etl.py",
        source_code=source,
        git_diff_since_green=None,
        failing_inputs={"rows": [{"cust_id": "C001"}]},
        regression_cases=[],
    )


def _diagnosis() -> Diagnosis:
    return Diagnosis(
        root_cause="upstream renamed customer_id to cust_id",
        category="schema_drift",
        confidence=0.9,
        affected_symbols=["clean_orders_fn"],
        reasoning="traceback shows KeyError on customer_id",
    )


def _patch() -> ProposedPatch:
    return ProposedPatch(
        edits=[
            Edit(
                file="retail_etl.py",
                old_string='row["customer_id"]',
                new_string='row.get("customer_id", row.get("cust_id"))',
            )
        ],
        rationale="tolerate the renamed column",
    )


def _report(status="pass") -> VerificationReport:
    return VerificationReport(
        checks=[
            CheckResult(check=c, status=status, detail={}, duration_ms=1)
            for c in ("repro", "fix", "regression", "scope")
        ],
        regression_passed=3,
        regression_total=3,
    )


def test_detector_prompt_includes_the_exception():
    prompt = detector_prompt(_evidence())
    assert "KeyError" in prompt
    assert "'customer_id'" in prompt


def test_detector_prompt_includes_source_and_logs():
    prompt = detector_prompt(_evidence())
    assert "clean_orders_fn" in prompt
    assert "line one" in prompt


def test_detector_prompt_is_bounded(monkeypatch):
    monkeypatch.setattr("orbit.agents.prompts.settings.max_prompt_chars", 500)
    prompt = detector_prompt(_evidence(source="x = 1\n" * 5000))
    assert len(prompt) < 3000


def test_fixer_prompt_carries_the_diagnosis():
    prompt = fixer_prompt(_evidence(), _diagnosis())
    assert "upstream renamed customer_id" in prompt
    assert "clean_orders_fn" in prompt


def test_fixer_prompt_names_the_bare_filename():
    """Edits address files by name, not path — the bundle is flat."""
    prompt = fixer_prompt(_evidence(), _diagnosis())
    assert "retail_etl.py" in prompt
    assert "dags/retail_etl.py\"" not in prompt.split("edit this file")[-1]


def test_fixer_system_demands_unique_old_string():
    """The single most common failure mode for a weaker model."""
    assert "exactly once" in FIXER_SYSTEM
    assert "old_string" in FIXER_SYSTEM


def test_fixer_system_forbids_symptom_suppression():
    lowered = FIXER_SYSTEM.lower()
    assert "except" in lowered or "swallow" in lowered


def test_reviewer_prompt_shows_patch_and_checks():
    prompt = reviewer_prompt(_diagnosis(), _patch(), _report())
    assert 'row.get("customer_id"' in prompt
    assert "regression" in prompt


def test_reviewer_prompt_reports_failed_checks():
    prompt = reviewer_prompt(_diagnosis(), _patch(), _report(status="fail"))
    assert "fail" in prompt


def test_reviewer_system_names_the_symptom_trap():
    lowered = REVIEWER_SYSTEM.lower()
    assert "root cause" in lowered
    assert "symptom" in lowered


def test_every_system_prompt_is_substantial():
    for text in (DETECTOR_SYSTEM, FIXER_SYSTEM, REVIEWER_SYSTEM):
        assert len(text) > 200


def test_prompts_are_deterministic():
    assert detector_prompt(_evidence()) == detector_prompt(_evidence())
    assert fixer_prompt(_evidence(), _diagnosis()) == fixer_prompt(
        _evidence(), _diagnosis()
    )
