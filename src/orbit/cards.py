"""Text for the two human-decision cards.

Pure functions over the contracts, so the wording is unit-testable without an
Airflow instance. A card is the only thing most people will read before
deciding, so it states the evidence rather than a verdict to be taken on trust.
"""

from __future__ import annotations

from orbit.contracts import ProposedPatch, ReviewVerdict, VerificationReport

TESTED = {
    "repro": "Reproduce the reported failure",
    "fix": "Patch clears the failure",
    "regression": "Recorded runs still pass",
    "scope": "Changes stay inside the diagnosed scope",
}
SAID = {"pass": "passed", "fail": "failed", "skipped": "not run"}
GLYPH = {"pass": "[+]", "fail": "[x]", "skipped": "[-]"}


def _by_name(report: VerificationReport) -> dict:
    return {c.check: c for c in report.checks}


def _findings(report: VerificationReport) -> str:
    lines = []
    for name, label in TESTED.items():
        check = _by_name(report).get(name)
        status = check.status if check else "skipped"
        note = (
            f" ({report.regression_passed} of {report.regression_total})"
            if name == "regression" and report.regression_total
            else ""
        )
        lines.append(
            f"  {GLYPH.get(status, '[-]')} {label}{note} — {SAID.get(status, status)}"
        )
    return "\n".join(lines)


def subject_line(dag_id: str, task_id: str, *, verified: bool) -> str:
    if verified:
        return f"Apply Orbit's verified fix for {task_id} in {dag_id}?"
    return f"Orbit could not verify a fix for {task_id} in {dag_id}"


def approval_body(
    report: VerificationReport, patch: ProposedPatch, diff: str
) -> str:
    against = (
        f" and re-ran it against {report.regression_total} recorded runs "
        "of this pipeline"
        if report.regression_total
        else ""
    )
    return (
        f"Orbit reproduced this failure, applied the patch below{against}. "
        f"All {len(report.checks)} checks cleared.\n\n"
        f"{_findings(report)}\n\n"
        f"Why this patch: {patch.rationale}\n\n"
        f"{diff}\n"
        "Approve commits this to a branch and reruns the failed task.\n"
        "Reject leaves your code and your pipeline untouched."
    )


def escalation_body(
    report: VerificationReport,
    verdict: ReviewVerdict,
    patch: ProposedPatch,
    diff: str,
) -> str:
    unresolved = [
        f"  {GLYPH.get(c.status, '[-]')} {TESTED.get(c.check, c.check)} — "
        f"{SAID.get(c.status, c.status)}"
        for c in report.checks
        if c.status != "pass"
    ]
    disagreements = (
        "\n".join(f"  - {d}" for d in verdict.disagreements)
        if verdict.disagreements
        else "  (none recorded)"
    )
    return (
        "Orbit is not asking you to trust this patch. "
        f"{len(unresolved)} of {len(report.checks)} checks did not pass:\n\n"
        + "\n".join(unresolved)
        + "\n\nReviewer verdict: "
        + verdict.verdict
        + "\n"
        + verdict.reasoning
        + "\n\nReviewer objections:\n"
        + disagreements
        + f"\n\nWhy the fixer proposed it: {patch.rationale}\n\n"
        + diff
        + "\nApply anyway commits it and reruns the task.\n"
        "Reject leaves everything untouched.\n"
        "Retry with more context sends it back for another attempt."
    )
