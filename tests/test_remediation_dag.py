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
