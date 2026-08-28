from __future__ import annotations

import difflib
from pathlib import Path

from orbit.contracts import ProposedPatch


class PatchApplicationError(RuntimeError):
    """An edit did not match, matched ambiguously, or produced bad Python."""


def _reindent(block: str, source_lines: list[str], start: int) -> str:
    """Re-anchor a block to the file's own indentation.

    Keeps each line's indentation *relative* to the block's first line, so a
    multi-line edit stays internally consistent.
    """
    block_lines = block.split("\n")
    shift = len(source_lines[start]) - len(source_lines[start].lstrip())
    shift -= len(block_lines[0]) - len(block_lines[0].lstrip())
    out = []
    for line in block_lines:
        if not line.strip():
            out.append(line)
        elif shift >= 0:
            out.append(" " * shift + line)
        else:
            out.append(line[-shift:] if line[:-shift].isspace() else line.lstrip())
    return "\n".join(out)


def _locate_ignoring_indent(text: str, old: str) -> tuple[str, str] | None:
    """Find `old` ignoring leading whitespace on every line.

    Models frequently reproduce a line with the wrong indentation. Match on
    content, then splice using the text actually in the file.
    """
    source_lines = text.split("\n")
    wanted = [line.strip() for line in old.split("\n")]
    matches = [
        i
        for i in range(len(source_lines) - len(wanted) + 1)
        if [line.strip() for line in source_lines[i : i + len(wanted)]] == wanted
    ]
    if len(matches) != 1:
        return None if not matches else ("", "")
    start = matches[0]
    return "\n".join(source_lines[start : start + len(wanted)]), str(start)


def _edited_text(root: Path, file: str, original: str, edits) -> str:
    text = original
    for edit in edits:
        if edit.old_string == edit.new_string:
            raise PatchApplicationError(f"{file}: old_string and new_string identical")

        old, new = edit.old_string, edit.new_string
        occurrences = text.count(old)

        if occurrences == 0:
            located = _locate_ignoring_indent(text, old)
            if located is None:
                raise PatchApplicationError(
                    f"{file}: old_string not found: {old[:60]!r}"
                )
            actual, start = located
            if not actual:
                raise PatchApplicationError(
                    f"{file}: old_string matched several places ignoring indentation, "
                    f"expected 1: {old[:60]!r}"
                )
            new = _reindent(new, text.split("\n"), int(start))
            old, occurrences = actual, 1

        if occurrences > 1:
            raise PatchApplicationError(
                f"{file}: old_string matched {occurrences} times, expected 1: "
                f"{old[:60]!r}"
            )
        text = text.replace(old, new, 1)
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
