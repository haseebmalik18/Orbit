from orbit.cards import approval_body, escalation_body, subject_line
from orbit.contracts import (
    CheckResult,
    Edit,
    ProposedPatch,
    ReviewVerdict,
    VerificationReport,
)


def _report(**overrides):
    statuses = {
        "repro": "pass",
        "fix": "pass",
        "regression": "pass",
        "scope": "pass",
    }
    statuses.update(overrides)
    return VerificationReport(
        checks=[
            CheckResult(check=name, status=status, detail={}, duration_ms=1000)
            for name, status in statuses.items()
        ],
        regression_passed=5 if statuses["regression"] == "pass" else 3,
        regression_total=5,
    )


def _patch():
    return ProposedPatch(
        edits=[
            Edit(
                file="retail_etl.py",
                old_string='row["customer_id"]',
                new_string='row.get("customer_id")',
            )
        ],
        rationale="map the renamed column",
    )


def _verdict(reasoning="the fix hides the symptom"):
    return ReviewVerdict(
        verdict="suppresses_symptom", reasoning=reasoning, disagreements=["drops rows"]
    )


DIFF = "--- a/retail_etl.py\n+++ b/retail_etl.py\n-old\n+new\n"


def test_approval_body_shows_the_diff():
    body = approval_body(_report(), _patch(), DIFF)
    assert "+new" in body


def test_approval_body_leads_with_the_replay_count():
    """The number of recorded runs re-executed is the reason to trust it."""
    assert "5 recorded runs" in approval_body(_report(), _patch(), DIFF)


def test_approval_body_says_what_each_button_does():
    body = approval_body(_report(), _patch(), DIFF)
    assert "Approve" in body and "Reject" in body


def test_escalation_body_names_the_checks_that_failed():
    """A card that just says "not verified" makes the human redo the work."""
    body = escalation_body(_report(regression="fail"), _verdict(), _patch(), DIFF)
    assert "Recorded runs still pass" in body
    assert "failed" in body


def test_escalation_body_does_not_claim_passing_checks_failed():
    body = escalation_body(_report(regression="fail"), _verdict(), _patch(), DIFF)
    reported = body.split("Reviewer")[0]
    assert "Reproduce the reported failure" not in reported


def test_escalation_body_names_a_skipped_check_as_not_run():
    body = escalation_body(_report(regression="skipped"), _verdict(), _patch(), DIFF)
    assert "not run" in body


def test_escalation_body_quotes_the_reviewer_verbatim():
    """Paraphrasing the reviewer is how a real objection gets lost."""
    body = escalation_body(
        _report(regression="fail"), _verdict("this silently drops rows"), _patch(), DIFF
    )
    assert "this silently drops rows" in body


def test_escalation_body_carries_the_reviewer_disagreements():
    body = escalation_body(_report(regression="fail"), _verdict(), _patch(), DIFF)
    assert "drops rows" in body


def test_subject_line_identifies_the_task():
    line = subject_line("retail_etl", "clean_orders", verified=True)
    assert "clean_orders" in line and "retail_etl" in line


def test_subject_line_distinguishes_the_two_paths():
    assert subject_line("d", "t", verified=True) != subject_line("d", "t", verified=False)


def test_escalation_says_so_when_the_reviewer_alone_objected():
    """The trap passes all four checks and is caught by judgment. Saying
    "0 of 4 checks did not pass" above an empty list reads like a bug and
    buries the actual reason."""
    body = escalation_body(_report(), _verdict(), _patch(), DIFF)
    assert "0 of 4" not in body
    assert "All 4 automated checks passed" in body


def test_escalation_still_counts_real_check_failures():
    body = escalation_body(_report(regression="fail"), _verdict(), _patch(), DIFF)
    assert "1 of 4" in body
