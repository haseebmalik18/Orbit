import pytest
from airflow.models import DagBag

EXPECTED_TASKS = {
    "collect_evidence",
    "diagnose",
    "record_diagnosis",
    "propose_fix",
    "record_patch",
    "verify",
    "review",
    "record_review",
    "decide",
    "stage_verified",
    "stage_escalated",
    "approve_verified_fix",
    "escalate_unverified",
    "record_approval",
    "route_escalation",
    "apply_patch",
    "rerun_failed_task",
    "close_unapplied",
}


@pytest.fixture(scope="module")
def dagbag():
    # include_examples was removed from DagBag in Airflow 3.3
    return DagBag(dag_folder="dags")


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_both_dags_are_registered(dagbag):
    assert "orbit_remediation" in dagbag.dags
    assert "retail_etl" in dagbag.dags


def test_remediation_dag_has_expected_task_graph(dagbag):
    dag = dagbag.dags["orbit_remediation"]
    assert set(dag.task_ids) == EXPECTED_TASKS


def test_decide_branches_to_both_outcomes(dagbag):
    dag = dagbag.dags["orbit_remediation"]
    assert dag.get_task("decide").downstream_task_ids == {
        "stage_verified",
        "stage_escalated",
    }


def test_agents_run_in_order(dagbag):
    """Each agent is followed by its record task, which feeds the next stage."""
    dag = dagbag.dags["orbit_remediation"]
    chain = [
        ("collect_evidence", "diagnose"),
        ("diagnose", "record_diagnosis"),
        ("record_diagnosis", "propose_fix"),
        ("propose_fix", "record_patch"),
        ("record_patch", "verify"),
        ("verify", "review"),
        ("review", "record_review"),
        ("record_review", "decide"),
    ]
    for upstream, downstream in chain:
        assert downstream in dag.get_task(upstream).downstream_task_ids, (
            f"{upstream} -> {downstream}"
        )


def test_record_tasks_follow_each_agent(dagbag):
    dag = dagbag.dags["orbit_remediation"]
    assert "record_diagnosis" in dag.get_task("diagnose").downstream_task_ids
    assert "record_patch" in dag.get_task("propose_fix").downstream_task_ids
    assert "record_review" in dag.get_task("review").downstream_task_ids


def test_real_mode_builds_llm_operators(monkeypatch):
    """`settings` is a module-level singleton built at import time, so the env
    var is already baked in — patch the attribute or this silently tests stubs."""
    monkeypatch.setattr("orbit.config.settings.use_stub_agents", False)
    dag = DagBag(dag_folder="dags").dags["orbit_remediation"]
    for agent in ("diagnose", "propose_fix", "review"):
        assert "LLM" in type(dag.get_task(agent)).__name__.upper(), agent


def test_llm_tasks_retry_but_verify_does_not(monkeypatch):
    """Providers 503 under load, so agent calls retry. Replay is deterministic
    and expensive — retrying it doubles the cost and changes nothing."""
    monkeypatch.setattr("orbit.config.settings.use_stub_agents", False)
    dag = DagBag(dag_folder="dags").dags["orbit_remediation"]
    for agent in ("diagnose", "propose_fix", "review"):
        assert dag.get_task(agent).retries >= 2, agent
    assert dag.get_task("verify").retries == 0


def test_verify_is_never_an_llm_call(monkeypatch):
    """If an LLM graded its own patch, "verified" would mean nothing."""
    for stub_mode in (True, False):
        monkeypatch.setattr("orbit.config.settings.use_stub_agents", stub_mode)
        dag = DagBag(dag_folder="dags").dags["orbit_remediation"]
        assert "LLM" not in type(dag.get_task("verify")).__name__.upper()


def test_task_ids_are_identical_in_stub_and_real_mode(monkeypatch):
    """The panel and the decision gate must not care which mode is running."""
    monkeypatch.setattr("orbit.config.settings.use_stub_agents", False)
    real = DagBag(dag_folder="dags").dags["orbit_remediation"]
    monkeypatch.setattr("orbit.config.settings.use_stub_agents", True)
    stub = DagBag(dag_folder="dags").dags["orbit_remediation"]
    assert set(real.task_ids) == set(stub.task_ids) == EXPECTED_TASKS


def test_remediation_dag_is_not_scheduled(dagbag):
    """It only ever runs when the listener triggers it."""
    assert dagbag.dags["orbit_remediation"].schedule is None


def test_apply_is_reachable_only_through_a_human_gate(dagbag):
    """The safety property of the whole project. Each task feeding apply_patch
    has a HITL operator upstream and demands all of its upstreams succeed, so a
    skipped gate — which is what Reject produces — stops the apply."""
    dag = dagbag.dags["orbit_remediation"]
    gates = {"approve_verified_fix", "escalate_unverified"}

    feeders = dag.get_task("apply_patch").upstream_task_ids
    assert feeders, "apply_patch has no upstream at all"
    for name in feeders:
        feeder = dag.get_task(name)
        assert feeder.upstream_task_ids & gates, f"{name} bypasses the human gate"
        assert feeder.trigger_rule == "all_success", (
            f"{name} could run without its gate succeeding"
        )


def test_rejecting_the_approval_skips_the_apply(dagbag):
    """ApprovalOperator skips its downstream on Reject, so this is structural
    rather than an if-statement inside the apply task."""
    from airflow.providers.standard.operators.hitl import ApprovalOperator

    dag = dagbag.dags["orbit_remediation"]
    assert isinstance(dag.get_task("approve_verified_fix"), ApprovalOperator)


def test_an_unanswered_approval_defaults_to_reject(dagbag):
    """Silence must never apply a patch."""
    dag = dagbag.dags["orbit_remediation"]
    approval = dag.get_task("approve_verified_fix")
    assert approval.defaults == ["Reject"]
    assert approval.response_timeout is not None


def test_the_escalation_offers_all_three_options(dagbag):
    dag = dagbag.dags["orbit_remediation"]
    assert dag.get_task("escalate_unverified").options == [
        "Apply anyway",
        "Reject",
        "Retry with more context",
    ]


def test_an_unanswered_escalation_defaults_to_reject(dagbag):
    dag = dagbag.dags["orbit_remediation"]
    assert dag.get_task("escalate_unverified").defaults == ["Reject"]


def test_the_rerun_happens_only_after_a_successful_apply(dagbag):
    """Default trigger rule, so a skipped or failed apply stops the rerun."""
    dag = dagbag.dags["orbit_remediation"]
    rerun = dag.get_task("rerun_failed_task")
    assert "apply_patch" in rerun.upstream_task_ids
    assert rerun.trigger_rule == "all_success"


def test_apply_depends_on_nothing_but_the_two_gates(dagbag):
    """Any other upstream lets a success-counting trigger rule fire the apply
    while the human's answer was Reject."""
    dag = dagbag.dags["orbit_remediation"]
    assert dag.get_task("apply_patch").upstream_task_ids == {
        "record_approval",
        "route_escalation",
    }
