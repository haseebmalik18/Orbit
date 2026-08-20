from __future__ import annotations

import ast
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from orbit.contracts import ProposedPatch


class PatchApplicationError(RuntimeError):
    """The patch would not apply, or produced unparseable Python."""


def _apply_patch(shadow: Path, unified_diff: str) -> None:
    result = subprocess.run(
        ["git", "apply", "-p1", "-"],
        input=unified_diff,
        capture_output=True,
        text=True,
        cwd=shadow,
    )
    if result.returncode != 0:
        raise PatchApplicationError(f"patch did not apply: {result.stderr.strip()}")


def _assert_parses(shadow: Path) -> None:
    for path in sorted(shadow.rglob("*.py")):
        try:
            ast.parse(path.read_text())
        except SyntaxError as exc:
            raise PatchApplicationError(
                f"patched file does not parse: {path.name}: {exc}"
            ) from exc


@contextmanager
def shadow_bundle(source_root: Path, patch: ProposedPatch | None) -> Iterator[Path]:
    """Yield a temp copy of the DAG bundle with `patch` applied.

    The production bundle is never written to. The yielded path is suitable for
    AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST.
    """
    temp_root = Path(tempfile.mkdtemp(prefix="orbit-shadow-"))
    shadow = temp_root / "dags"
    try:
        shutil.copytree(source_root, shadow)
        if patch is not None:
            _apply_patch(shadow, patch.unified_diff)
            _assert_parses(shadow)
        yield shadow
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
