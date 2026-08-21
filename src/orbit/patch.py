from __future__ import annotations

import difflib
from pathlib import Path

from orbit.contracts import ProposedPatch


class PatchApplicationError(RuntimeError):
    """An edit did not match, matched ambiguously, or produced bad Python."""


def _edited_text(root: Path, file: str, original: str, edits) -> str:
    text = original
    for edit in edits:
        occurrences = text.count(edit.old_string)
        if occurrences == 0:
            raise PatchApplicationError(
                f"{file}: old_string not found: {edit.old_string[:60]!r}"
            )
        if occurrences > 1:
            raise PatchApplicationError(
                f"{file}: old_string matched {occurrences} times, expected 1: "
                f"{edit.old_string[:60]!r}"
            )
        if edit.old_string == edit.new_string:
            raise PatchApplicationError(f"{file}: old_string and new_string identical")
        text = text.replace(edit.old_string, edit.new_string, 1)
    return text


def _read(root: Path, file: str) -> str:
    path = root / Path(file).name
    if not path.exists():
        raise PatchApplicationError(f"no such file in bundle: {file}")
    return path.read_text()


def _by_file(patch: ProposedPatch) -> dict[str, list]:
    grouped: dict[str, list] = {}
    for edit in patch.edits:
        grouped.setdefault(edit.file, []).append(edit)
    return grouped


def apply_edits(root: Path, patch: ProposedPatch) -> None:
    """Apply every edit in place. Raises rather than partially applying."""
    rewritten: dict[Path, str] = {}
    for file, edits in _by_file(patch).items():
        rewritten[root / Path(file).name] = _edited_text(
            root, file, _read(root, file), edits
        )
    for path, text in rewritten.items():
        path.write_text(text)


def render_diff(root: Path, patch: ProposedPatch) -> str:
    """Produce a unified diff for display, without touching the source."""
    chunks: list[str] = []
    for file, edits in _by_file(patch).items():
        name = Path(file).name
        original = _read(root, file)
        updated = _edited_text(root, file, original, edits)
        chunks.extend(
            difflib.unified_diff(
                original.splitlines(keepends=True),
                updated.splitlines(keepends=True),
                fromfile=f"a/{name}",
                tofile=f"b/{name}",
            )
        )
    return "".join(chunks)
