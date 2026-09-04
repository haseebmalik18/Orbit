import pytest

from orbit.store.repository import InvalidTransition, Repository


@pytest.fixture
def repo(tmp_path):
    return Repository(tmp_path / "orbit.db")


def test_create_and_fetch_incident(repo):
    incident_id = repo.create_incident(
        dag_id="retail_etl",
        task_id="clean_orders",
        run_id="run-1",
        try_number=1,
        exception_type="KeyError",
        exception_message="'customer_id'",
    )
    incident = repo.get_incident(incident_id)
    assert incident["status"] == "detected"
    assert incident["dag_id"] == "retail_etl"
    assert incident["detected_at"] is not None


def test_status_advances_through_state_machine(repo):
    incident_id = repo.create_incident("d", "t", "r", 1, "E", "m")
    for status in ("diagnosing", "verifying", "awaiting_human", "applying", "resolved"):
        repo.set_status(incident_id, status)
    assert repo.get_incident(incident_id)["status"] == "resolved"
    assert repo.get_incident(incident_id)["resolved_at"] is not None


def test_invalid_status_is_rejected(repo):
    incident_id = repo.create_incident("d", "t", "r", 1, "E", "m")
    with pytest.raises(InvalidTransition):
        repo.set_status(incident_id, "banana")


def test_terminal_status_cannot_be_reopened(repo):
    incident_id = repo.create_incident("d", "t", "r", 1, "E", "m")
    repo.set_status(incident_id, "failed")
    with pytest.raises(InvalidTransition):
        repo.set_status(incident_id, "diagnosing")


def test_messages_preserve_insertion_order(repo):
    incident_id = repo.create_incident("d", "t", "r", 1, "E", "m")
    repo.add_message(incident_id, "detector", "assistant", "first", model="stub")
    repo.add_message(incident_id, "fixer", "assistant", "second", model="stub")
    repo.add_message(incident_id, "reviewer", "assistant", "third", model="stub")
    assert [m["content"] for m in repo.get_messages(incident_id)] == [
        "first",
        "second",
        "third",
    ]


def test_checks_persist_with_detail_and_duration(repo):
    incident_id = repo.create_incident("d", "t", "r", 1, "E", "m")
    repo.add_check(incident_id, "repro", "pass", {"exception": "KeyError"}, 812)
    check = repo.get_checks(incident_id)[0]
    assert check["check"] == "repro"
    assert check["detail"]["exception"] == "KeyError"
    assert check["duration_ms"] == 812


def test_list_incidents_filters_by_run(repo):
    repo.create_incident("d", "t", "run-a", 1, "E", "m")
    repo.create_incident("d", "t", "run-b", 1, "E", "m")
    assert len(repo.list_incidents(run_id="run-a")) == 1


def test_stats_counts_resolved_and_escalated(repo):
    """Counted off the resolution the pipeline writes, not off a human
    decision. Counting decisions left the panel showing 0 verified next to an
    incident whose own badge said verified — nothing records a decision until
    a human answers the approval card."""
    a = repo.create_incident("d", "t", "r1", 1, "E", "m")
    b = repo.create_incident("d", "t", "r2", 1, "E", "m")
    repo.set_resolution(a, "verified_awaiting_approval")
    repo.set_resolution(b, "escalated_not_verified")
    stats = repo.stats()
    assert stats["verified"] == 1
    assert stats["escalated"] == 1
    assert stats["total_incidents"] == 2


def test_stats_needs_no_human_decision(repo):
    """No decision is recorded here — the count must still land."""
    a = repo.create_incident("d", "t", "r1", 1, "E", "m")
    repo.set_resolution(a, "verified_awaiting_approval")
    assert repo.stats()["verified"] == 1


def test_an_incident_still_running_counts_in_neither_bucket(repo):
    repo.create_incident("d", "t", "r1", 1, "E", "m")
    stats = repo.stats()
    assert (stats["verified"], stats["escalated"]) == (0, 0)
    assert stats["total_incidents"] == 1


def test_set_status_on_unknown_incident_raises(repo):
    with pytest.raises(InvalidTransition):
        repo.set_status("inc-nope", "diagnosing")


def test_schema_is_idempotent(tmp_path):
    Repository(tmp_path / "orbit.db")
    Repository(tmp_path / "orbit.db")


def test_creates_parent_directory(tmp_path):
    repo = Repository(tmp_path / "nested" / "dir" / "orbit.db")
    assert repo.db_path.parent.is_dir()
