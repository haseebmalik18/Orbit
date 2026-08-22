"""Orbit's remediation pipeline: an Airflow DAG that repairs Airflow DAGs."""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.sdk import dag, task

from orbit.agents import prompts, stubs
from orbit.agents.budget import check_and_count
from orbit.config import settings
from orbit.contracts import (
    Diagnosis,
    Evidence,
    ProposedPatch,
    ReviewVerdict,
    VerificationReport,
)
from orbit.evidence import collect
from orbit.store.repository import Repository
from orbit.trigger import AirflowClient
from orbit.verifier.runner import verify as run_verification

DAG_SOURCE_ROOT = Path(__file__).parent


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
    @task
    def diagnose(evidence: dict) -> dict:
        check_and_count(_repo(), evidence["incident_id"], "detector")
        return stubs.diagnose(Evidence.model_validate(evidence)).model_dump()

    @task
    def propose_fix(evidence: dict, diagnosis: dict) -> dict:
        check_and_count(_repo(), evidence["incident_id"], "fixer")
        return stubs.propose_fix(
            Evidence.model_validate(evidence), Diagnosis.model_validate(diagnosis)
        ).model_dump()

    @task
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
    the parsed object straight into XCom."""

    @task.llm(
        llm_conn_id=settings.detector_conn_id,
        model_id=settings.detector_model,
        system_prompt=prompts.DETECTOR_SYSTEM,
        output_type=Diagnosis,
    )
    def diagnose(evidence: dict) -> str:
        check_and_count(_repo(), evidence["incident_id"], "detector")
        return prompts.detector_prompt(Evidence.model_validate(evidence))

    @task.llm(
        llm_conn_id=settings.fixer_conn_id,
        model_id=settings.fixer_model,
        system_prompt=prompts.FIXER_SYSTEM,
        output_type=ProposedPatch,
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
    @task
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

    @task
    def record_diagnosis(evidence: dict, diagnosis: dict) -> dict:
        _repo().add_message(
            evidence["incident_id"],
            "detector",
            "assistant",
            Diagnosis.model_validate(diagnosis).reasoning,
            _model_for("detector"),
        )
        return diagnosis

    @task
    def record_patch(evidence: dict, patch: dict) -> dict:
        _repo().add_message(
            evidence["incident_id"],
            "fixer",
            "assistant",
            ProposedPatch.model_validate(patch).rationale,
            _model_for("fixer"),
        )
        return patch

    @task
    def record_review(evidence: dict, verdict: dict) -> dict:
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
        return verdict

    @task
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

    @task
    def stage_verified(evidence: dict) -> None:
        repo = _repo()
        repo.set_status(evidence["incident_id"], "awaiting_human")
        repo.set_resolution(evidence["incident_id"], "verified_awaiting_approval")

    @task
    def stage_escalated(evidence: dict) -> None:
        repo = _repo()
        repo.set_status(evidence["incident_id"], "awaiting_human")
        repo.set_resolution(evidence["incident_id"], "escalated_not_verified")

    evidence = collect_evidence()
    diagnosis = record_diagnosis(evidence, diagnose(evidence))
    patch = record_patch(evidence, propose_fix(evidence, diagnosis))
    report = verify(evidence, patch, diagnosis)
    verdict = record_review(evidence, review(evidence, diagnosis, patch, report))
    decide(report, verdict) >> [stage_verified(evidence), stage_escalated(evidence)]


orbit_remediation()
