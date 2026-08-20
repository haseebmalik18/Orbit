import json
import subprocess

from orbit.contracts import Case
from orbit.verifier.replay import replay


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


def test_missing_sentinel_yields_none_output(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: MockCompleted(0, "no sentinel here")
    )
    result = replay("d", "t", _case(), "/tmp/shadow", 10)
    assert result.succeeded is True
    assert result.output is None


def test_failed_replay_captures_exception_type(monkeypatch):
    stderr = "Traceback (most recent call last):\nKeyError: 'customer_id'\n"
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: MockCompleted(1, "", stderr))
    result = replay("retail_etl", "clean_orders", _case(), "/tmp/shadow", 10)
    assert result.succeeded is False
    assert result.exception_type == "KeyError"
    assert result.output is None


def test_failed_replay_reads_structured_json_logs(monkeypatch):
    """Airflow 3.3 emits structured logs, so exc_type is available directly."""
    stderr = '{"event":"task failed","exc_type":"ValueError","exc_value":"bad float"}'
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: MockCompleted(1, "", stderr))
    result = replay("d", "t", _case(), "/tmp/shadow", 10)
    assert result.exception_type == "ValueError"


def test_unrecognisable_failure_yields_none_exception_type(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: MockCompleted(1, "", "segfault")
    )
    result = replay("d", "t", _case(), "/tmp/shadow", 10)
    assert result.succeeded is False
    assert result.exception_type is None


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
