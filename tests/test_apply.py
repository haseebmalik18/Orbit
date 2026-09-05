import subprocess

import pytest

from orbit.apply import ApplyFailed, apply_on_branch
from orbit.contracts import Edit, ProposedPatch

BEFORE = """def clean_orders_fn(rows):
    return [{"id": row["customer_id"]} for row in rows]
"""
AFTER = """def clean_orders_fn(rows):
    return [{"id": row.get("customer_id")} for row in rows]
"""


def git(root, *args):
    return subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "project"
    (root / "dags").mkdir(parents=True)
    (root / "dags" / "retail_etl.py").write_text(BEFORE)
    (root / "untouched.txt").write_text("leave me alone\n")
    git(root.parent, "init", "-q", str(root))
    git(root, "add", "-A")
    # Identity passed per-command rather than configured, so the repository is
    # left without one — which is the state inside the Airflow containers.
    git(
        root,
        "-c", "user.email=setup@test",
        "-c", "user.name=Setup",
        "commit", "-q", "-m", "initial",
    )
    return root


def _patch(old='row["customer_id"]', new='row.get("customer_id")'):
    return ProposedPatch(
        edits=[Edit(file="retail_etl.py", old_string=old, new_string=new)],
        rationale="handle the renamed column",
    )


def test_the_commit_lands_on_its_own_branch(repo):
    result = apply_on_branch(repo, _patch(), "inc-1", subdir="dags")
    assert result["branch"] == "orbit/fix-inc-1"
    assert git(repo, "rev-parse", result["branch"]) == result["sha"]


def test_the_commit_contains_the_edit(repo):
    result = apply_on_branch(repo, _patch(), "inc-1", subdir="dags")
    shown = git(repo, "show", result["sha"])
    assert 'row.get("customer_id")' in shown
    assert "retail_etl.py" in shown


def test_head_does_not_move(repo):
    """Approving a patch must not drag the operator's repository onto another
    branch as a side effect."""
    before = git(repo, "rev-parse", "HEAD")
    branch_before = git(repo, "rev-parse", "--abbrev-ref", "HEAD")
    apply_on_branch(repo, _patch(), "inc-1", subdir="dags")
    assert git(repo, "rev-parse", "HEAD") == before
    assert git(repo, "rev-parse", "--abbrev-ref", "HEAD") == branch_before


def test_the_working_tree_carries_the_fix(repo):
    """The DAG processor re-parses from disk, so the rerun only goes green if
    the edit is actually on the filesystem."""
    apply_on_branch(repo, _patch(), "inc-1", subdir="dags")
    assert (repo / "dags" / "retail_etl.py").read_text() == AFTER


def test_a_patch_that_does_not_match_leaves_nothing_behind(repo):
    before_refs = git(repo, "for-each-ref", "--format=%(refname)")
    with pytest.raises(ApplyFailed):
        apply_on_branch(repo, _patch(old="text that is not there"), "inc-2", subdir="dags")
    assert (repo / "dags" / "retail_etl.py").read_text() == BEFORE
    assert git(repo, "for-each-ref", "--format=%(refname)") == before_refs


def test_only_the_named_files_are_committed(repo):
    """A dirty tree is normal. Sweeping it into Orbit's commit is not."""
    (repo / "untouched.txt").write_text("edited by a human, mid-thought\n")
    result = apply_on_branch(repo, _patch(), "inc-1", subdir="dags")
    files = git(repo, "show", "--name-only", "--format=", result["sha"]).split()
    assert files == ["dags/retail_etl.py"]


def test_the_human_edit_survives(repo):
    (repo / "untouched.txt").write_text("edited by a human, mid-thought\n")
    apply_on_branch(repo, _patch(), "inc-1", subdir="dags")
    assert (repo / "untouched.txt").read_text() == "edited by a human, mid-thought\n"


def test_the_incident_id_is_in_the_message(repo):
    result = apply_on_branch(repo, _patch(), "inc-1", subdir="dags")
    assert "inc-1" in git(repo, "show", "-s", "--format=%B", result["sha"])


def test_applying_outside_a_repository_fails_loudly(tmp_path):
    root = tmp_path / "bare"
    (root / "dags").mkdir(parents=True)
    (root / "dags" / "retail_etl.py").write_text(BEFORE)
    with pytest.raises(ApplyFailed):
        apply_on_branch(root, _patch(), "inc-1", subdir="dags")


def test_the_commit_is_authored_by_orbit(repo):
    """Containers carry no git identity, so the commit has to bring its own or
    git refuses with "Author identity unknown"."""
    result = apply_on_branch(repo, _patch(), "inc-1", subdir="dags")
    assert git(repo, "show", "-s", "--format=%an", result["sha"]) == "Orbit"
