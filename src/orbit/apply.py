"""Commit an approved patch to its own branch without disturbing the checkout.

A patch reaches this module only after a human has approved it. Two properties
matter more than anything else here: the operator's own HEAD must not move as a
side effect of approving, and a failure must leave neither a commit nor a
half-edited file behind.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

from orbit.contracts import ProposedPatch
from orbit.patch import PatchApplicationError, apply_edits


class ApplyFailed(RuntimeError):
    pass


def _git(root: Path, *args: str, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        # The worker does not own the bind-mounted checkout, which trips git's
        # dubious-ownership guard. Scoped to this path rather than set globally.
        ["git", "-c", f"safe.directory={root}", "-C", str(root), *args],
        capture_output=True,
        text=True,
        env={**os.environ, **(env or {})},
    )
    if result.returncode != 0:
        raise ApplyFailed(
            "git " + " ".join(args) + " failed: " + (result.stderr.strip() or "no output")
        )
    return result.stdout.strip()


def _commit_without_checkout(
    root: Path, paths: list[str], message: str, branch: str
) -> str:
    """Build a commit from the working-tree copies of `paths` alone.

    Everything goes through a scratch index, so the repository's real index and
    HEAD are untouched and any other dirty files stay out of the commit.
    """
    with tempfile.TemporaryDirectory() as scratch:
        index = {"GIT_INDEX_FILE": str(Path(scratch) / "index")}
        _git(root, "read-tree", "HEAD", env=index)
        _git(root, "update-index", "--add", "--", *paths, env=index)
        tree = _git(root, "write-tree", env=index)

    head = _git(root, "rev-parse", "HEAD")
    sha = _git(root, "commit-tree", tree, "-p", head, "-m", message)
    _git(root, "branch", "-f", branch, sha)
    return sha


def apply_on_branch(
    repo_root: Path | str,
    patch: ProposedPatch,
    incident_id: str,
    subdir: str = "dags",
) -> dict[str, str]:
    """Apply `patch` to the working tree and record it as a commit on a branch.

    The edit is left on disk on purpose: the DAG processor parses from the
    filesystem, so a rerun only goes green if the file actually changed.
    """
    root = Path(repo_root)
    source_root = root / subdir

    _git(root, "rev-parse", "--git-dir")

    touched = sorted({edit.file for edit in patch.edits})
    originals = {name: (source_root / name).read_text() for name in touched}

    branch = f"orbit/fix-{incident_id}"
    existed = branch in _git(root, "for-each-ref", "--format=%(refname:short)").split()

    try:
        apply_edits(source_root, patch)
        message = (
            f"Apply Orbit fix for {incident_id}\n\n{patch.rationale}\n\n"
            "Proposed by Orbit and verified against recorded runs before a human "
            "approved it."
        )
        sha = _commit_without_checkout(
            root, [f"{subdir}/{name}" for name in touched], message, branch
        )
    except (PatchApplicationError, ApplyFailed, OSError) as exc:
        for name, text in originals.items():
            (source_root / name).write_text(text)
        if not existed:
            subprocess.run(
                ["git", "-C", str(root), "branch", "-D", branch],
                capture_output=True,
                text=True,
            )
        raise ApplyFailed(f"could not apply the patch for {incident_id}: {exc}") from exc

    return {"branch": branch, "sha": sha}
