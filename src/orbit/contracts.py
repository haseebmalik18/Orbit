from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, computed_field

CheckName = Literal["repro", "fix", "regression", "scope"]
CheckStatus = Literal["pass", "fail", "skipped"]
REQUIRED_CHECKS = frozenset({"repro", "fix", "regression", "scope"})


class Case(BaseModel):
    """One recorded (inputs, expected_output) pair from a past run."""

    run_id: str
    inputs: dict[str, Any]
    expected_output: Any


class Evidence(BaseModel):
    incident_id: str
    dag_id: str
    task_id: str
    run_id: str
    exception_type: str
    exception_message: str
    log_tail: list[str]
    source_path: str
    source_code: str
    git_diff_since_green: str | None
    failing_inputs: dict[str, Any]
    regression_cases: list[Case]


class Diagnosis(BaseModel):
    root_cause: str
    category: Literal[
        "schema_drift",
        "type_error",
        "config",
        "dependency",
        "resource",
        "logic",
        "unknown",
    ]
    # self-reported by the model; advisory only, never gates the decision
    confidence: float
    affected_symbols: list[str]
    reasoning: str


class Edit(BaseModel):
    """One exact string replacement.

    Models generate these far more reliably than unified diffs — no hunk
    headers, no line arithmetic. The diff is rendered from them afterwards.
    """

    file: str
    old_string: str
    new_string: str


class ProposedPatch(BaseModel):
    edits: list[Edit]
    rationale: str

    @computed_field
    @property
    def files_touched(self) -> list[str]:
        seen: list[str] = []
        for edit in self.edits:
            if edit.file not in seen:
                seen.append(edit.file)
        return seen


class CheckResult(BaseModel):
    check: CheckName
    status: CheckStatus
    detail: dict[str, Any]
    duration_ms: int


class VerificationReport(BaseModel):
    checks: list[CheckResult]
    regression_passed: int
    regression_total: int

    @computed_field
    @property
    def all_passed(self) -> bool:
        by_name = {c.check: c.status for c in self.checks}
        return all(by_name.get(name) == "pass" for name in REQUIRED_CHECKS)


class ReviewVerdict(BaseModel):
    verdict: Literal[
        "addresses_root_cause",
        "suppresses_symptom",
        "out_of_scope",
        "insufficient_evidence",
    ]
    reasoning: str
    disagreements: list[str]
