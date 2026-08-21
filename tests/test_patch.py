import pytest

from orbit.contracts import Edit, ProposedPatch
from orbit.patch import PatchApplicationError, apply_edits, render_diff

SOURCE = '''def clean(rows):
    out = []
    for row in rows:
        out.append({"id": row["id"], "amount": float(row["amount"])})
    return out
'''


@pytest.fixture
def root(tmp_path):
    (tmp_path / "m.py").write_text(SOURCE)
    return tmp_path


def _patch(old, new, file="m.py"):
    return ProposedPatch(
        edits=[Edit(file=file, old_string=old, new_string=new)], rationale="r"
    )


def test_edit_is_applied_exactly(root):
    apply_edits(root, _patch('row["id"]', 'row.get("id", row.get("ident"))'))
    assert 'row.get("id", row.get("ident"))' in (root / "m.py").read_text()


def test_only_the_matched_text_changes(root):
    apply_edits(root, _patch("    return out", "    return list(out)"))
    text = (root / "m.py").read_text()
    assert "return list(out)" in text
    assert 'float(row["amount"])' in text


def test_missing_old_string_raises(root):
    with pytest.raises(PatchApplicationError, match="not found"):
        apply_edits(root, _patch("this text is absent", "x"))


def test_ambiguous_old_string_raises(root):
    """Multiple matches mean we cannot know which one the model meant."""
    with pytest.raises(PatchApplicationError, match="expected 1"):
        apply_edits(root, _patch("row", "r"))


def test_missing_file_raises(root):
    with pytest.raises(PatchApplicationError, match="no such file"):
        apply_edits(root, _patch("out", "x", file="ghost.py"))


def test_noop_edit_raises(root):
    with pytest.raises(PatchApplicationError, match="identical"):
        apply_edits(root, _patch("    return out", "    return out"))


def test_multiple_edits_all_applied(root):
    patch = ProposedPatch(
        edits=[
            Edit(file="m.py", old_string='row["id"]', new_string='row["ident"]'),
            Edit(file="m.py", old_string="    return out", new_string="    return []"),
        ],
        rationale="two changes",
    )
    apply_edits(root, patch)
    text = (root / "m.py").read_text()
    assert 'row["ident"]' in text
    assert "return []" in text


def test_files_touched_is_derived(root):
    patch = ProposedPatch(
        edits=[
            Edit(file="a.py", old_string="x", new_string="y"),
            Edit(file="b.py", old_string="x", new_string="y"),
            Edit(file="a.py", old_string="p", new_string="q"),
        ],
        rationale="r",
    )
    assert patch.files_touched == ["a.py", "b.py"]


def test_render_diff_is_a_valid_unified_diff(root):
    diff = render_diff(root, _patch('row["id"]', 'row["ident"]'))
    assert diff.startswith("--- a/m.py")
    assert "+++ b/m.py" in diff
    assert "@@" in diff
    assert '-        out.append({"id": row["id"]' in diff
    assert '+        out.append({"id": row["ident"]' in diff


def test_render_diff_does_not_modify_the_source(root):
    before = (root / "m.py").read_text()
    render_diff(root, _patch('row["id"]', 'row["ident"]'))
    assert (root / "m.py").read_text() == before


def test_render_diff_covers_every_file(root):
    (root / "b.py").write_text("VALUE = 1\n")
    patch = ProposedPatch(
        edits=[
            Edit(file="m.py", old_string='row["id"]', new_string='row["ident"]'),
            Edit(file="b.py", old_string="VALUE = 1", new_string="VALUE = 2"),
        ],
        rationale="r",
    )
    diff = render_diff(root, patch)
    assert "--- a/m.py" in diff
    assert "--- a/b.py" in diff
