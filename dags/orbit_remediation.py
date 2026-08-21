"""Orbit's remediation pipeline: an Airflow DAG that repairs Airflow DAGs."""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.sdk import dag, task

from orbit.agents import stubs
from orbit.config import settings
from orbit.contracts import (
    Diagnosis,
    Evidence,
    ProposedPatch,
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

    @task
    def diagnose(evidence: dict) -> dict:
        result = stubs.diagnose(Evidence.model_validate(evidence))
        _repo().add_message(
            evidence["incident_id"], "detector", "assistant", result.reasoning, "stub"
        )
        return result.model_dump()

    @task
    def propose_fix(evidence: dict, diagnosis: dict) -> dict:
        result = stubs.propose_fix(
            Evidence.model_validate(evidence), Diagnosis.model_validate(diagnosis)
        )
        _repo().add_message(
            evidence["incident_id"], "fixer", "assistant", result.rationale, "stub"
        )
        return result.model_dump()

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

    @task
    def review(evidence: dict, diagnosis: dict, patch: dict, report: dict) -> dict:
        verdict = stubs.review(
            Diagnosis.model_validate(diagnosis),
            ProposedPatch.model_validate(patch),
            VerificationReport.model_validate(report),
        )
        _repo().add_message(
            evidence["incident_id"], "reviewer", "assistant", verdict.reasoning, "stub"
        )
        return verdict.model_dump()

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
    diagnosis = diagnose(evidence)
    patch = propose_fix(evidence, diagnosis)
    report = verify(evidence, patch, diagnosis)
    verdict = review(evidence, diagnosis, patch, report)
    decide(report, verdict) >> [stage_verified(evidence), stage_escalated(evidence)]


orbit_remediation()
