from __future__ import annotations

import ast
import time
from pathlib import Path

from orbit.contracts import CheckResult, Diagnosis, ProposedPatch


def _top_level_defs(source: str) -> dict[str, str]:
    tree = ast.parse(source)
    out: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            out[node.name] = ast.dump(node)
    return out


def changed_symbols(before: str, after: str) -> set[str]:
    """Top-level defs whose AST differs. Reformatting does not count."""
    old, new = _top_level_defs(before), _top_level_defs(after)
    changed = {name for name, body in new.items() if old.get(name) != body}
    return changed | (set(old) - set(new))


def check_scope(
    patch: ProposedPatch,
    diagnosis: Diagnosis,
    source_root: Path,
    shadow_root: Path,
) -> CheckResult:
    """Confirm the patch touches only what the diagnosis claimed it would."""
    started = time.perf_counter()
    declared = set(diagnosis.affected_symbols)
    actually_changed: set[str] = set()

    for relative in patch.files_touched:
        name = Path(relative).name
        original, patched = source_root / name, shadow_root / name
        if not original.exists() or not patched.exists():
            continue
        actually_changed |= changed_symbols(original.read_text(), patched.read_text())

    undeclared = actually_changed - declared
    return CheckResult(
        check="scope",
        status="fail" if undeclared else "pass",
        detail={
            "declared": sorted(declared),
            "changed": sorted(actually_changed),
            "undeclared": sorted(undeclared),
        },
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
