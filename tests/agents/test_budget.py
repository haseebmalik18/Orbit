import pytest

from orbit.agents.budget import BudgetExceeded, check_and_count, truncate
from orbit.store.repository import Repository


@pytest.fixture
def repo(tmp_path):
    return Repository(tmp_path / "orbit.db")


def _incident(repo):
    return repo.create_incident("retail_etl", "clean_orders", "r", 1, "KeyError", "m")


def test_calls_under_the_ceiling_are_allowed(repo, monkeypatch):
    monkeypatch.setattr("orbit.agents.budget.settings.max_agent_calls_per_incident", 3)
    incident_id = _incident(repo)
    for agent in ("detector", "fixer", "reviewer"):
        check_and_count(repo, incident_id, agent)
    assert len(repo.get_messages(incident_id)) == 3


def test_exceeding_the_ceiling_raises(repo, monkeypatch):
    """A runaway loop must stop billing, not keep going."""
    monkeypatch.setattr("orbit.agents.budget.settings.max_agent_calls_per_incident", 2)
    incident_id = _incident(repo)
    check_and_count(repo, incident_id, "detector")
    check_and_count(repo, incident_id, "fixer")
    with pytest.raises(BudgetExceeded, match="2"):
        check_and_count(repo, incident_id, "reviewer")


def test_ceiling_is_per_incident_not_global(repo, monkeypatch):
    monkeypatch.setattr("orbit.agents.budget.settings.max_agent_calls_per_incident", 1)
    first, second = _incident(repo), _incident(repo)
    check_and_count(repo, first, "detector")
    check_and_count(repo, second, "detector")
    with pytest.raises(BudgetExceeded):
        check_and_count(repo, first, "fixer")


def test_budget_records_the_attempt_before_the_call(repo, monkeypatch):
    """Counted on attempt, so a call that hangs still consumes budget."""
    monkeypatch.setattr("orbit.agents.budget.settings.max_agent_calls_per_incident", 5)
    incident_id = _incident(repo)
    check_and_count(repo, incident_id, "detector")
    messages = repo.get_messages(incident_id)
    assert messages[0]["agent"] == "detector"
    assert messages[0]["role"] == "attempt"


def test_attempts_do_not_pollute_the_transcript(repo, monkeypatch):
    """The panel shows assistant messages; attempts are bookkeeping."""
    monkeypatch.setattr("orbit.agents.budget.settings.max_agent_calls_per_incident", 5)
    incident_id = _incident(repo)
    check_and_count(repo, incident_id, "detector")
    repo.add_message(incident_id, "detector", "assistant", "real output", "groq:x")
    visible = [m for m in repo.get_messages(incident_id) if m["role"] == "assistant"]
    assert len(visible) == 1
    assert visible[0]["content"] == "real output"


def test_truncate_keeps_short_text_intact():
    assert truncate("hello", 100) == "hello"


def test_truncate_marks_what_it_cut():
    result = truncate("x" * 500, 100)
    assert len(result) < 200
    assert "truncated" in result


def test_truncate_keeps_the_tail_not_the_head():
    """Tracebacks put the useful part last."""
    text = "noise\n" * 100 + "KeyError: 'customer_id'"
    assert "KeyError: 'customer_id'" in truncate(text, 120)
