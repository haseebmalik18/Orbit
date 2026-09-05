"""Reading a human's answer off a HITL operator's XCom.

The payload is not flat: `chosen_options` is a list even for a single-choice
card, and `responded_by_user` is a user object rather than a name. Both get
written to the store, so they are narrowed to plain strings here.
"""

from __future__ import annotations

from typing import Any


def chosen_option(response: Any, fallback: str) -> str:
    """The one option a person picked, or `fallback` when nobody answered."""
    if not isinstance(response, dict):
        return fallback
    options = response.get("chosen_options") or []
    return options[0] if options else fallback


def responder(response: Any, fallback: str = "timeout") -> str:
    """Who answered, as a string the store can hold."""
    if not isinstance(response, dict):
        return fallback
    who = response.get("responded_by_user")
    if isinstance(who, dict):
        return str(who.get("name") or who.get("id") or fallback)
    return str(who) if who else fallback
