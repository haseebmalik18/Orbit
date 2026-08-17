import pytest
from airflow.models import DagBag

EXPECTED_TASKS = {
    "collect_evidence",
    "diagnose",
    "propose_fix",
    "verify",
    "review",
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
    dag = dagbag.dags["orbit_remediation"]
    assert "diagnose" in dag.get_task("collect_evidence").downstream_task_ids
    assert "propose_fix" in dag.get_task("diagnose").downstream_task_ids
    assert "verify" in dag.get_task("propose_fix").downstream_task_ids
    assert "review" in dag.get_task("verify").downstream_task_ids


def test_remediation_dag_is_not_scheduled(dagbag):
    """It only ever runs when the listener triggers it."""
    assert dagbag.dags["orbit_remediation"].schedule is None
