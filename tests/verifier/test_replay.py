import json
import subprocess
from pathlib import Path

import pytest

from orbit.contracts import Case
from orbit.verifier.replay import replay


@pytest.fixture(autouse=True)
def stub_template_db(monkeypatch, tmp_path):
    """Keep every test off the real `airflow db migrate`."""
    template = tmp_path / "template.db"
    template.write_bytes(b"pretend sqlite")
    monkeypatch.setattr("orbit.verifier.replay._ensure_template_db", lambda: template)
    return template


class MockCompleted:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _case(inputs=None):
    return Case(
        run_id="run-1",
        inputs=inputs if inputs is not None else {"rows": []},
        expected_output={"row_count": 0},
    )


def _payload(value):
    return f"__ORBIT_RESULT__{json.dumps(value)}__ORBIT_END__"


def test_successful_replay_parses_output(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: MockCompleted(0, _payload({"row_count": 8}))
    )
    result = replay("retail_etl", "clean_orders", _case(), "/tmp/shadow", 10)
    assert result.succeeded is True
    assert result.output == {"row_count": 8}
    assert result.timed_out is False


def test_output_is_found_amid_surrounding_log_noise(monkeypatch):
    noisy = f"[info] starting\n{_payload([1, 2])}\n[info] done\n"
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: MockCompleted(0, noisy))
    result = replay("d", "t", _case(), "/tmp/shadow", 10)
    assert result.output == [1, 2]


def test_missing_sentinel_means_the_task_did_not_complete(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: MockCompleted(0, "no sentinel here")
    )
    result = replay("d", "t", _case(), "/tmp/shadow", 10)
    assert result.succeeded is False
    assert result.output is None


def test_task_failure_is_detected_despite_zero_exit_code(monkeypatch):
    """`airflow tasks test` exits 0 even when the task raises, so the sentinel
    is the only trustworthy completion signal."""
    stdout = "[info] running\nKeyError: 'customer_id'\nnew_state=failed\n"
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: MockCompleted(0, stdout, ""))
    result = replay("retail_etl", "clean_orders", _case(), "/tmp/shadow", 10)
    assert result.succeeded is False
    assert result.exception_type == "KeyError"
    assert result.output is None


def test_exception_type_is_read_from_stdout(monkeypatch):
    """Airflow 3.3 writes the traceback to stdout; stderr comes back empty."""
    stdout = "Traceback (most recent call last):\nValueError: bad float\n"
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: MockCompleted(0, stdout, ""))
    assert replay("d", "t", _case(), "/tmp/shadow", 10).exception_type == "ValueError"


def test_structured_json_exception_is_read(monkeypatch):
    stdout = '{"event":"task failed","exc_type":"TypeError","exc_value":"nope"}'
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: MockCompleted(0, stdout, ""))
    assert replay("d", "t", _case(), "/tmp/shadow", 10).exception_type == "TypeError"


def test_nonzero_exit_without_sentinel_is_a_failure(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: MockCompleted(1, "", "segfault")
    )
    result = replay("d", "t", _case(), "/tmp/shadow", 10)
    assert result.succeeded is False
    assert result.exception_type is None


def test_sentinel_with_null_payload_still_counts_as_success(monkeypatch):
    """A task returning None completed; it did not fail."""
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: MockCompleted(0, _payload(None), "")
    )
    result = replay("d", "t", _case(), "/tmp/shadow", 10)
    assert result.succeeded is True
    assert result.output is None


def test_timeout_is_recorded_not_raised(monkeypatch):
    def explode(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="airflow", timeout=1)

    monkeypatch.setattr(subprocess, "run", explode)
    result = replay("retail_etl", "clean_orders", _case(), "/tmp/shadow", 1)
    assert result.timed_out is True
    assert result.succeeded is False
    assert result.exception_type == "Timeout"


def test_bundle_env_points_at_the_shadow(monkeypatch):
    captured = {}

    def capture(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs["env"]
        return MockCompleted(0, _payload(None))

    monkeypatch.setattr(subprocess, "run", capture)
    replay("retail_etl", "clean_orders", _case(), "/tmp/shadow-xyz", 10)

    bundle_config = captured["env"]["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"]
    assert "/tmp/shadow-xyz" in bundle_config
    assert "LocalDagBundle" in bundle_config
    assert json.loads(bundle_config)[0]["kwargs"]["path"] == "/tmp/shadow-xyz"


def test_replay_uses_a_throwaway_metadata_db(monkeypatch):
    """`airflow tasks test` writes task_instance rows, so it must not be
    pointed at the production metadata database."""
    monkeypatch.setenv(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "postgresql://prod/airflow"
    )
    captured = {}

    def capture(cmd, **kwargs):
        captured["env"] = kwargs["env"]
        return MockCompleted(0, _payload(None))

    monkeypatch.setattr(subprocess, "run", capture)
    replay("d", "t", _case(), "/tmp/shadow", 10)

    conn = captured["env"]["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert conn.startswith("sqlite:///")
    assert "prod" not in conn


def test_scratch_db_is_removed_after_replay(monkeypatch):
    captured = {}

    def capture(cmd, **kwargs):
        conn = kwargs["env"]["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
        captured["path"] = conn.removeprefix("sqlite:///")
        return MockCompleted(0, _payload(None))

    monkeypatch.setattr(subprocess, "run", capture)
    replay("d", "t", _case(), "/tmp/shadow", 10)
    assert not Path(captured["path"]).exists()


def test_command_is_airflow_tasks_test(monkeypatch):
    captured = {}

    def capture(cmd, **kwargs):
        captured["cmd"] = cmd
        return MockCompleted(0, _payload(None))

    monkeypatch.setattr(subprocess, "run", capture)
    replay("retail_etl", "clean_orders", _case(), "/tmp/shadow", 10)
    assert captured["cmd"][:5] == [
        "airflow",
        "tasks",
        "test",
        "retail_etl",
        "clean_orders",
    ]


def test_recorded_inputs_are_passed_through_env(monkeypatch):
    captured = {}

    def capture(cmd, **kwargs):
        captured["env"] = kwargs["env"]
        return MockCompleted(0, _payload(None))

    monkeypatch.setattr(subprocess, "run", capture)
    rows = [{"customer_id": "C001", "amount": "10.00"}]
    replay("d", "t", _case({"rows": rows}), "/tmp/shadow", 10)
    assert captured["env"]["ORBIT_REPLAY_MODE"] == "1"
    assert json.loads(captured["env"]["ORBIT_REPLAY_INPUTS"]) == {"rows": rows}


def test_duration_is_recorded(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: MockCompleted(0, _payload(None))
    )
    result = replay("d", "t", _case(), "/tmp/shadow", 10)
    assert result.duration_ms >= 0
