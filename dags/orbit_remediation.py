"""Orbit's remediation pipeline: an Airflow DAG that repairs Airflow DAGs."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.hitl import ApprovalOperator, HITLOperator
from airflow.sdk import dag, get_current_context, task

from orbit.agents import prompts, stubs
from orbit.agents.budget import check_and_count
from orbit.apply import apply_on_branch
from orbit.cards import approval_body, escalation_body, subject_line
from orbit.config import settings
from orbit.contracts import (
    Diagnosis,
    Evidence,
    ProposedPatch,
    ReviewVerdict,
    VerificationReport,
)
from orbit.evidence import collect
from orbit.patch import render_diff
from orbit.store.repository import Repository
from orbit.trigger import AirflowClient
from orbit.verifier.runner import verify as run_verification

DAG_SOURCE_ROOT = Path(__file__).parent

APPLY_ANYWAY = "Apply anyway"
REJECT = "Reject"
RETRY = "Retry with more context"
# The uncertain path asks a different question from the confident one. That
# difference is the point, so the options are not shared.
ESCALATION_OPTIONS = [APPLY_ANYWAY, REJECT, RETRY]


def _repo() -> Repository:
    return Repository(settings.db_path)


def _client() -> AirflowClient:
    return AirflowClient(
        settings.airflow_base_url,
        settings.airflow_api_token,
        settings.airflow_username,
        settings.airflow_password,
    )


def _stub_agent_tasks():
    @task(multiple_outputs=False)
    def diagnose(evidence: dict) -> dict:
        check_and_count(_repo(), evidence["incident_id"], "detector")
        return stubs.diagnose(Evidence.model_validate(evidence)).model_dump()

    @task(multiple_outputs=False)
    def propose_fix(evidence: dict, diagnosis: dict) -> dict:
        check_and_count(_repo(), evidence["incident_id"], "fixer")
        return stubs.propose_fix(
            Evidence.model_validate(evidence), Diagnosis.model_validate(diagnosis)
        ).model_dump()

    @task(multiple_outputs=False)
    def review(evidence: dict, diagnosis: dict, patch: dict, report: dict) -> dict:
        check_and_count(_repo(), evidence["incident_id"], "reviewer")
        return stubs.review(
            Diagnosis.model_validate(diagnosis),
            ProposedPatch.model_validate(patch),
            VerificationReport.model_validate(report),
        ).model_dump()

    return diagnose, propose_fix, review


def _llm_agent_tasks():
    """Real agents. The decorated body returns the prompt; `output_type` puts
    the parsed object straight into XCom.

    Providers 503 under load, so every agent call retries. The budget guard
    counts each attempt, which keeps a retry storm bounded.
    """
    retry = {"retries": 2, "retry_delay": timedelta(seconds=15)}

    @task.llm(
        llm_conn_id=settings.detector_conn_id,
        model_id=settings.detector_model,
        system_prompt=prompts.DETECTOR_SYSTEM,
        output_type=Diagnosis,
        **retry,
    )
    def diagnose(evidence: dict) -> str:
        check_and_count(_repo(), evidence["incident_id"], "detector")
        return prompts.detector_prompt(Evidence.model_validate(evidence))

    @task.llm(
        llm_conn_id=settings.fixer_conn_id,
        model_id=settings.fixer_model,
        system_prompt=prompts.FIXER_SYSTEM,
        output_type=ProposedPatch,
        **retry,
    )
    def propose_fix(evidence: dict, diagnosis: dict) -> str:
        check_and_count(_repo(), evidence["incident_id"], "fixer")
        return prompts.fixer_prompt(
            Evidence.model_validate(evidence), Diagnosis.model_validate(diagnosis)
        )

    @task.llm(
        llm_conn_id=settings.reviewer_conn_id,
        model_id=settings.reviewer_model,
        system_prompt=prompts.REVIEWER_SYSTEM,
        output_type=ReviewVerdict,
        **retry,
    )
    def review(evidence: dict, diagnosis: dict, patch: dict, report: dict) -> str:
        check_and_count(_repo(), evidence["incident_id"], "reviewer")
        return prompts.reviewer_prompt(
            Diagnosis.model_validate(diagnosis),
            ProposedPatch.model_validate(patch),
            VerificationReport.model_validate(report),
        )

    return diagnose, propose_fix, review


@dag(
    dag_id="orbit_remediation",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    tags=["orbit"],
)
def orbit_remediation():
    @task(multiple_outputs=False)
    def collect_evidence(**context) -> dict:
        incident_id = context["dag_run"].conf["incident_id"]
        repo = _repo()
        repo.set_status(incident_id, "diagnosing")
        return collect(incident_id, repo, _client(), DAG_SOURCE_ROOT).model_dump()

    diagnose, propose_fix, review = (
        _stub_agent_tasks() if settings.use_stub_agents else _llm_agent_tasks()
    )

    def _model_for(agent: str) -> str:
        if settings.use_stub_agents:
            return "stub"
        return {
            "detector": settings.detector_model,
            "fixer": settings.fixer_model,
            "reviewer": settings.reviewer_model,
        }[agent]

    @task(multiple_outputs=False)
    def record_diagnosis(evidence: dict, diagnosis) -> dict:
        parsed = Diagnosis.model_validate(diagnosis)
        _repo().add_message(
            evidence["incident_id"],
            "detector",
            "assistant",
            parsed.reasoning,
            _model_for("detector"),
        )
        return parsed.model_dump()

    @task(multiple_outputs=False)
    def record_patch(evidence: dict, patch) -> dict:
        parsed = ProposedPatch.model_validate(patch)
        _repo().add_message(
            evidence["incident_id"],
            "fixer",
            "assistant",
            parsed.rationale,
            _model_for("fixer"),
        )
        return parsed.model_dump()

    @task(multiple_outputs=False)
    def record_review(evidence: dict, verdict) -> dict:
        parsed = ReviewVerdict.model_validate(verdict)
        content = parsed.reasoning
        if parsed.disagreements:
            content += "\n" + "\n".join(f"- {d}" for d in parsed.disagreements)
        _repo().add_message(
            evidence["incident_id"],
            "reviewer",
            "assistant",
            content,
            _model_for("reviewer"),
        )
        return parsed.model_dump()

    @task(multiple_outputs=False)
    def verify(evidence: dict, patch: dict, diagnosis: dict) -> dict:
        repo = _repo()
        evidence_model = Evidence.model_validate(evidence)
        repo.set_status(evidence_model.incident_id, "verifying")
        report = run_verification(
            evidence_model,
            ProposedPatch.model_validate(patch),
            Diagnosis.model_validate(diagnosis),
            DAG_SOURCE_ROOT,
        )
        for check in report.checks:
            repo.add_check(
                evidence_model.incident_id,
                check.check,
                check.status,
                check.detail,
                check.duration_ms,
            )
        return report.model_dump()

    @task.branch
    def decide(report: dict, verdict: dict) -> str:
        verified = (
            VerificationReport.model_validate(report).all_passed
            and verdict["verdict"] == "addresses_root_cause"
        )
        return "stage_verified" if verified else "stage_escalated"

    @task(multiple_outputs=False)
    def stage_verified(evidence: dict, report: dict, patch: dict) -> dict:
        repo = _repo()
        repo.set_status(evidence["incident_id"], "awaiting_human")
        repo.set_resolution(evidence["incident_id"], "verified_awaiting_approval")
        parsed = ProposedPatch.model_validate(patch)
        return {
            "subject": subject_line(
                evidence["dag_id"], evidence["task_id"], verified=True
            ),
            "body": approval_body(
                VerificationReport.model_validate(report),
                parsed,
                render_diff(DAG_SOURCE_ROOT, parsed),
            ),
        }

    @task(multiple_outputs=False)
    def stage_escalated(evidence: dict, report: dict, patch: dict, verdict: dict) -> dict:
        repo = _repo()
        repo.set_status(evidence["incident_id"], "awaiting_human")
        repo.set_resolution(evidence["incident_id"], "escalated_not_verified")
        parsed = ProposedPatch.model_validate(patch)
        return {
            "subject": subject_line(
                evidence["dag_id"], evidence["task_id"], verified=False
            ),
            "body": escalation_body(
                VerificationReport.model_validate(report),
                ReviewVerdict.model_validate(verdict),
                parsed,
                render_diff(DAG_SOURCE_ROOT, parsed),
            ),
        }

    approve_verified_fix = ApprovalOperator(
        task_id="approve_verified_fix",
        subject="{{ ti.xcom_pull(task_ids='stage_verified')['subject'] }}",
        body="{{ ti.xcom_pull(task_ids='stage_verified')['body'] }}",
        # silence must never apply a patch
        defaults=ApprovalOperator.REJECT,
        response_timeout=timedelta(minutes=settings.hitl_timeout_minutes),
    )

    escalate_unverified = HITLOperator(
        task_id="escalate_unverified",
        subject="{{ ti.xcom_pull(task_ids='stage_escalated')['subject'] }}",
        body="{{ ti.xcom_pull(task_ids='stage_escalated')['body'] }}",
        options=ESCALATION_OPTIONS,
        defaults=REJECT,
        response_timeout=timedelta(minutes=settings.hitl_timeout_minutes),
    )

    @task.branch
    def route_escalation(evidence: dict, response: dict) -> str:
        chosen = (response.get("chosen_options") or [REJECT])[0]
        _repo().record_decision(
            evidence["incident_id"],
            "escalated",
            chosen,
            response.get("responded_by_user") or "timeout",
        )
        return "apply_patch" if chosen == APPLY_ANYWAY else "close_unapplied"

    @task
    def record_approval(evidence: dict, response: dict) -> None:
        chosen = (response.get("chosen_options") or [REJECT])[0]
        _repo().record_decision(
            evidence["incident_id"],
            "verified",
            chosen,
            response.get("responded_by_user") or "timeout",
        )

    # Takes no arguments on purpose. Passing evidence and patch in would make
    # collect_evidence and record_patch upstreams of this task, and a trigger
    # rule counting successes would then fire it even when the human said no.
    # Its only upstreams are the two gates.
    @task(trigger_rule="one_success", multiple_outputs=False)
    def apply_patch() -> dict:
        context = get_current_context()
        evidence = context["ti"].xcom_pull(task_ids="collect_evidence")
        patch = context["ti"].xcom_pull(task_ids="record_patch")
        repo = _repo()
        incident_id = evidence["incident_id"]
        repo.set_status(incident_id, "applying")
        commit = apply_on_branch(
            settings.repo_root,
            ProposedPatch.model_validate(patch),
            incident_id,
            subdir=settings.dag_subdir,
        )
        repo.add_message(
            incident_id,
            "orbit",
            "assistant",
            f"Committed {commit['sha'][:12]} on {commit['branch']}.",
            "",
        )
        return commit

    @task
    def rerun_failed_task(evidence: dict, commit: dict) -> None:
        _client().clear_task_instance(
            evidence["dag_id"], evidence["run_id"], evidence["task_id"]
        )
        _repo().set_status(evidence["incident_id"], "resolved")

    @task
    def close_unapplied(evidence: dict) -> None:
        _repo().set_status(evidence["incident_id"], "resolved")

    evidence = collect_evidence()
    diagnosis = record_diagnosis(evidence, diagnose(evidence))
    patch = record_patch(evidence, propose_fix(evidence, diagnosis))
    report = verify(evidence, patch, diagnosis)
    verdict = record_review(evidence, review(evidence, diagnosis, patch, report))

    verified_card = stage_verified(evidence, report, patch)
    escalated_card = stage_escalated(evidence, report, patch, verdict)
    decide(report, verdict) >> [verified_card, escalated_card]

    verified_card >> approve_verified_fix
    escalated_card >> escalate_unverified

    # Reject skips everything downstream of the approval, so no patch reaches
    # apply_patch without someone having said yes.
    approved = record_approval(evidence, approve_verified_fix.output)
    routed = route_escalation(evidence, escalate_unverified.output)

    commit = apply_patch()
    approved >> commit
    routed >> commit
    routed >> close_unapplied(evidence)
    commit >> rerun_failed_task(evidence, commit)


orbit_remediation()
