from __future__ import annotations

import logging
from pathlib import Path

from orbit.config import settings
from orbit.contracts import (
    REQUIRED_CHECKS,
    Case,
    CheckResult,
    Diagnosis,
    Evidence,
    ProposedPatch,
    VerificationReport,
)
from orbit.verifier.bundle import PatchApplicationError, shadow_bundle
from orbit.verifier.compare import compare
from orbit.verifier.replay import replay
from orbit.verifier.scope import check_scope

log = logging.getLogger(__name__)


def _skipped(name: str, reason: str) -> CheckResult:
    return CheckResult(
        check=name, status="skipped", detail={"reason": reason}, duration_ms=0
    )


def _failed(name: str, reason: str) -> CheckResult:
    return CheckResult(
        check=name, status="fail", detail={"error": reason}, duration_ms=0
    )


def verify(
    evidence: Evidence,
    patch: ProposedPatch,
    diagnosis: Diagnosis,
    source_root: Path,
    volatile_fields: list[str] | None = None,
) -> VerificationReport:
    """Run the four checks. Never raises; failures become failed checks."""
    volatile = volatile_fields if volatile_fields is not None else evidence.volatile_fields
    checks: list[CheckResult] = []

    # A task that cannot be replayed is never treated as verified. Skipping is
    # not passing, so this routes to escalation rather than silently approving.
    if not evidence.replayable:
        reason = "task did not opt in with orbit_replayable=True"
        return VerificationReport(
            checks=[_skipped(n, reason) for n in REQUIRED_CHECKS],
            regression_passed=0,
            regression_total=0,
        )

    failing_case = Case(
        run_id=evidence.run_id, inputs=evidence.failing_inputs, expected_output=None
    )

    # 1. repro against UNPATCHED code. If the bug will not reproduce, nothing
    # downstream means anything.
    with shadow_bundle(source_root, None) as clean_shadow:
        result = replay(
            evidence.dag_id,
            evidence.task_id,
            failing_case,
            str(clean_shadow),
            settings.replay_timeout_s,
        )
    reproduced = not result.succeeded
    checks.append(
        CheckResult(
            check="repro",
            status="pass" if reproduced else "fail",
            detail={
                "expected_exception": evidence.exception_type,
                "observed_exception": result.exception_type,
                "timed_out": result.timed_out,
            },
            duration_ms=result.duration_ms,
        )
    )
    if not reproduced:
        reason = "bug did not reproduce"
        checks += [_skipped(n, reason) for n in ("fix", "regression", "scope")]
        return VerificationReport(checks=checks, regression_passed=0, regression_total=0)

    passed = 0
    total = len(evidence.regression_cases)
    try:
        with shadow_bundle(source_root, patch) as patched_shadow:
            fix_result = replay(
                evidence.dag_id,
                evidence.task_id,
                failing_case,
                str(patched_shadow),
                settings.replay_timeout_s,
            )
            checks.append(
                CheckResult(
                    check="fix",
                    status="pass" if fix_result.succeeded else "fail",
                    detail={
                        "exception": fix_result.exception_type,
                        "timed_out": fix_result.timed_out,
                    },
                    duration_ms=fix_result.duration_ms,
                )
            )

            differences: list[dict] = []
            elapsed = 0
            for case in evidence.regression_cases:
                case_result = replay(
                    evidence.dag_id,
                    evidence.task_id,
                    case,
                    str(patched_shadow),
                    settings.replay_timeout_s,
                )
                elapsed += case_result.duration_ms
                if not case_result.succeeded:
                    differences.append(
                        {"run_id": case.run_id, "error": case_result.exception_type}
                    )
                    continue
                comparison = compare(case_result.output, case.expected_output, volatile)
                if comparison["match"]:
                    passed += 1
                else:
                    differences.append(
                        {
                            "run_id": case.run_id,
                            "differences": comparison["differences"],
                        }
                    )

            if total == 0:
                checks.append(_skipped("regression", "no recorded successful runs"))
            else:
                checks.append(
                    CheckResult(
                        check="regression",
                        status="pass" if passed == total else "fail",
                        detail={
                            "passed": passed,
                            "total": total,
                            "diffs": differences,
                        },
                        duration_ms=elapsed,
                    )
                )

            checks.append(check_scope(patch, diagnosis, source_root, patched_shadow))

    except PatchApplicationError as exc:
        present = {c.check for c in checks}
        checks += [
            _failed(n, str(exc))
            for n in ("fix", "regression", "scope")
            if n not in present
        ]
        return VerificationReport(checks=checks, regression_passed=0, regression_total=0)

    return VerificationReport(
        checks=checks, regression_passed=passed, regression_total=total
    )
