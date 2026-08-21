from __future__ import annotations

from orbit.config import settings

TRUNCATION_MARKER = "\n...[truncated]...\n"


class BudgetExceeded(RuntimeError):
    """This incident has used its allowance of agent calls."""


def check_and_count(repo, incident_id: str, agent: str) -> None:
    """Reserve one agent call, or refuse.

    Counted before the call rather than after, so a call that hangs or crashes
    still consumes budget — otherwise a crash loop bills forever.
    """
    attempts = [m for m in repo.get_messages(incident_id) if m["role"] == "attempt"]
    ceiling = settings.max_agent_calls_per_incident
    if len(attempts) >= ceiling:
        raise BudgetExceeded(
            f"incident {incident_id} already used {len(attempts)} of {ceiling} calls"
        )
    repo.add_message(incident_id, agent, "attempt", f"calling {agent}")


def truncate(text: str, limit: int) -> str:
    """Keep the tail — tracebacks put the useful part last."""
    if len(text) <= limit:
        return text
    return TRUNCATION_MARKER + text[-limit:]
