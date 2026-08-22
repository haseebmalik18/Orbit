from __future__ import annotations

import json

from orbit.agents.budget import truncate
from orbit.config import settings
from orbit.contracts import Diagnosis, Evidence, ProposedPatch, VerificationReport

DETECTOR_SYSTEM = """You diagnose failed Apache Airflow tasks.

You are given the exception, the tail of the task log, and the source of the DAG
module that failed. Identify the root cause — the reason the task failed, not the
line that raised.

Set `affected_symbols` to the top-level function or class names that must change
to fix this. Copy the names exactly as they appear in the source. A downstream
check compares your list against what the fix actually touches, so listing
symbols that do not need to change will cause a false failure.

`confidence` is your own estimate from 0 to 1. It is recorded but never used to
decide anything, so report it honestly rather than optimistically.

Diagnose only. Do not propose a fix."""

FIXER_SYSTEM = """You repair failed Apache Airflow tasks by editing their source.

Return one or more edits. Each edit replaces `old_string` with `new_string` in
the named file.

Rules for `old_string`, in order of importance:
1. It must appear exactly once in the file. If the text you want to change is
   not unique, extend it with surrounding lines until it is.
2. Copy it verbatim from the source you were given, including indentation. A
   single wrong space means the edit will not apply.
3. Keep it as small as it can be while staying unique.

Fix the root cause you were given. Do not fix anything else, do not reformat,
and do not refactor — an automated check compares what you touched against the
diagnosis, and unrelated changes fail it.

Never suppress a symptom to make an error disappear. A bare `except: pass`, a
broad try/except that swallows the exception, or silently dropping rows that
fail to parse are all wrong answers even though they stop the error. The
pipeline's own history is replayed against your change, so a fix that quietly
alters results on data that previously worked will be caught and rejected."""

REVIEWER_SYSTEM = """You review a proposed fix for a failed Airflow task.

You are given the diagnosis, the proposed edits, and the results of automated
verification that already ran.

Answer one question: does this patch address the stated root cause, or does it
suppress the symptom?

Suppressing the symptom means making the error stop without fixing what caused
it — swallowing an exception, catching and ignoring, dropping the rows that
fail, widening a type until nothing can fail. These are wrong even when every
automated check passes.

Use the verdicts precisely:
- `addresses_root_cause` — the patch fixes the cause named in the diagnosis
- `suppresses_symptom` — it hides the error rather than fixing it
- `out_of_scope` — it changes things the diagnosis did not call for
- `insufficient_evidence` — you cannot tell from what you were given

Put every specific concern in `disagreements`, one per entry, phrased so a human
reading only that list understands the objection. These are shown verbatim to
the person deciding whether to apply the patch. An empty list means you have no
concerns at all."""


def _fmt(value: object) -> str:
    return json.dumps(value, indent=2, default=str)


def detector_prompt(evidence: Evidence) -> str:
    budget = settings.max_prompt_chars
    log_tail = "\n".join(evidence.log_tail)
    return f"""A task failed in Airflow.

DAG: {evidence.dag_id}
Task: {evidence.task_id}
Exception: {evidence.exception_type}: {evidence.exception_message}

--- log tail ---
{truncate(log_tail, budget // 3)}

--- source of {evidence.source_path} ---
{truncate(evidence.source_code, budget // 2)}

--- inputs the task received ---
{truncate(_fmt(evidence.failing_inputs), budget // 6)}

Diagnose the root cause."""


def fixer_prompt(evidence: Evidence, diagnosis: Diagnosis) -> str:
    budget = settings.max_prompt_chars
    filename = evidence.source_path.split("/")[-1]
    symbols = ", ".join(diagnosis.affected_symbols) or "(none named)"
    return f"""Fix this failing Airflow task.

Diagnosis: {diagnosis.root_cause}
Category: {diagnosis.category}
Symbols to change: {symbols}
Reasoning: {diagnosis.reasoning}

Exception: {evidence.exception_type}: {evidence.exception_message}

--- source to edit this file ---
{truncate(evidence.source_code, budget // 2)}

--- inputs the task received ---
{truncate(_fmt(evidence.failing_inputs), budget // 6)}

Return the edits that fix the root cause. Set every edit's `file` to exactly
{filename} — the bundle is flat, so use the bare name and not a path."""


def reviewer_prompt(
    diagnosis: Diagnosis, patch: ProposedPatch, report: VerificationReport
) -> str:
    edits = "\n\n".join(
        f"file: {e.file}\n--- replace ---\n{e.old_string}\n--- with ---\n{e.new_string}"
        for e in patch.edits
    )
    checks = "\n".join(f"  {c.check}: {c.status}" for c in report.checks)
    symbols = ", ".join(diagnosis.affected_symbols)
    return f"""Review this proposed fix.

--- diagnosis ---
{diagnosis.root_cause}
(category: {diagnosis.category}; symbols: {symbols})

--- proposed edits ---
{truncate(edits, settings.max_prompt_chars // 2)}

Stated rationale: {patch.rationale}

--- automated verification already run ---
{checks}
  regression cases passed: {report.regression_passed}/{report.regression_total}

Does this address the root cause, or suppress the symptom?"""
