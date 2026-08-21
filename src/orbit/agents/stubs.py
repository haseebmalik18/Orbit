"""Deterministic stand-ins for the LLM agents.

These let the pipeline and the verifier be built and tested without API calls or
nondeterminism. Plan 2 swaps them for @task.llm calls behind the same signatures.
"""

from __future__ import annotations

from orbit.contracts import (
    Diagnosis,
    Edit,
    Evidence,
    ProposedPatch,
    ReviewVerdict,
    VerificationReport,
)

CATEGORY_BY_EXCEPTION = {
    "KeyError": "schema_drift",
    "TypeError": "type_error",
    "ValueError": "logic",
}

RENAME_EDIT = Edit(
    file="retail_etl.py",
    old_string='"customer_id": row["customer_id"],',
    new_string='"customer_id": row.get("customer_id", row.get("cust_id")),',
)


def diagnose(evidence: Evidence) -> Diagnosis:
    category = CATEGORY_BY_EXCEPTION.get(evidence.exception_type, "unknown")
    return Diagnosis(
        root_cause=f"{evidence.exception_type}: {evidence.exception_message}",
        category=category,
        confidence=0.9,
        affected_symbols=["clean_orders_fn"],
        reasoning="Stubbed diagnosis derived from the exception type.",
    )


def propose_fix(evidence: Evidence, diagnosis: Diagnosis) -> ProposedPatch:
    return ProposedPatch(
        edits=[RENAME_EDIT],
        rationale="Stubbed patch: tolerate the renamed column.",
    )


def review(
    diagnosis: Diagnosis, patch: ProposedPatch, report: VerificationReport
) -> ReviewVerdict:
    if report.all_passed:
        return ReviewVerdict(
            verdict="addresses_root_cause",
            reasoning="Stubbed review: all checks passed.",
            disagreements=[],
        )
    failed = [c.check for c in report.checks if c.status != "pass"]
    return ReviewVerdict(
        verdict="insufficient_evidence",
        reasoning="Stubbed review: verification did not pass.",
        disagreements=[f"check '{name}' did not pass" for name in failed],
    )
