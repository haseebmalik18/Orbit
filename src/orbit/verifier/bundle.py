from __future__ import annotations

import ast
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from orbit.contracts import ProposedPatch
from orbit.patch import PatchApplicationError, apply_edits

__all__ = ["PatchApplicationError", "shadow_bundle"]


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
            apply_edits(shadow, patch)
            _assert_parses(shadow)
        yield shadow
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
