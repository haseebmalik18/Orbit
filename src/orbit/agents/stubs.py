"""Deterministic stand-ins for the LLM agents.

These let the pipeline and the verifier be built and tested without API calls or
nondeterminism. Plan 2 swaps them for @task.llm calls behind the same signatures.
"""

from __future__ import annotations

from orbit.contracts import (
    Diagnosis,
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

RENAME_PATCH = '''--- a/retail_etl.py
+++ b/retail_etl.py
@@ -35,7 +35,7 @@ def clean_orders_fn(rows: list[dict]) -> list[dict]:
         cleaned.append(
             {
                 "order_id": row["order_id"],
-                "customer_id": row["customer_id"],
+                "customer_id": row.get("customer_id", row.get("cust_id")),
                 "order_date": row["order_date"],
                 "amount": float(row["amount"]),
             }
'''


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
        unified_diff=RENAME_PATCH,
        files_touched=["retail_etl.py"],
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
