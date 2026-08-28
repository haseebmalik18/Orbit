"""A model that returns something unusable must fail the incident.

The danger is not a crash — it is a malformed response being coerced into
something that looks verified and reaches a human as "ready to apply".
"""

import pytest
from pydantic import ValidationError

from orbit.contracts import Diagnosis, Evidence, ProposedPatch, ReviewVerdict
from orbit.patch import PatchApplicationError, apply_edits


def test_diagnosis_with_an_invented_category_is_rejected():
    with pytest.raises(ValidationError):
        Diagnosis.model_validate(
            {
                "root_cause": "x",
                "category": "vibes",
                "confidence": 0.9,
                "affected_symbols": [],
                "reasoning": "y",
            }
        )


def test_verdict_with_an_invented_value_is_rejected():
    with pytest.raises(ValidationError):
        ReviewVerdict.model_validate(
            {"verdict": "looks_fine_to_me", "reasoning": "y", "disagreements": []}
        )


def test_patch_missing_new_string_is_rejected():
    with pytest.raises(ValidationError):
        ProposedPatch.model_validate(
            {"edits": [{"file": "m.py", "old_string": "a"}], "rationale": "r"}
        )


def test_patch_with_prose_instead_of_edits_is_rejected():
    """A model that explains the fix rather than returning edits."""
    with pytest.raises(ValidationError):
        ProposedPatch.model_validate(
            {"edits": "change customer_id to cust_id", "rationale": "r"}
        )


def test_evidence_missing_a_field_is_rejected():
    with pytest.raises(ValidationError):
        Evidence.model_validate({"incident_id": "inc-1", "dag_id": "d"})


def test_hallucinated_file_fails_rather_than_creating_it(tmp_path):
    """A model naming a file that isn't in the bundle must not write one."""
    (tmp_path / "real.py").write_text("VALUE = 1\n")
    patch = ProposedPatch.model_validate(
        {
            "edits": [
                {"file": "imaginary.py", "old_string": "VALUE = 1", "new_string": "V = 2"}
            ],
            "rationale": "r",
        }
    )
    with pytest.raises(PatchApplicationError, match="no such file"):
        apply_edits(tmp_path, patch)
    assert not (tmp_path / "imaginary.py").exists()


def test_hallucinated_source_text_fails(tmp_path):
    """A model editing code it imagined rather than code that exists."""
    (tmp_path / "m.py").write_text("VALUE = 1\n")
    patch = ProposedPatch.model_validate(
        {
            "edits": [
                {
                    "file": "m.py",
                    "old_string": "def process(records):",
                    "new_string": "def process(records, strict=False):",
                }
            ],
            "rationale": "r",
        }
    )
    with pytest.raises(PatchApplicationError, match="not found"):
        apply_edits(tmp_path, patch)
    assert (tmp_path / "m.py").read_text() == "VALUE = 1\n"


def test_a_partially_valid_patch_applies_nothing(tmp_path):
    """First edit good, second bad — the file must be left untouched."""
    (tmp_path / "m.py").write_text("A = 1\nB = 2\n")
    patch = ProposedPatch.model_validate(
        {
            "edits": [
                {"file": "m.py", "old_string": "A = 1", "new_string": "A = 9"},
                {"file": "m.py", "old_string": "NOPE", "new_string": "X"},
            ],
            "rationale": "r",
        }
    )
    with pytest.raises(PatchApplicationError):
        apply_edits(tmp_path, patch)
    assert (tmp_path / "m.py").read_text() == "A = 1\nB = 2\n"
