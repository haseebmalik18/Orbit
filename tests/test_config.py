from orbit.config import Settings


def test_settings_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("ORBIT_DB_PATH", str(tmp_path / "orbit.db"))
    monkeypatch.delenv("ORBIT_WATCHED_DAGS", raising=False)
    s = Settings()
    assert s.db_path == tmp_path / "orbit.db"
    assert s.regression_case_count == 5
    assert s.replay_timeout_s == 120
    assert s.log_tail_lines == 200
    assert s.watched_dag_ids == ["retail_etl"]


def test_watched_dags_parsed_from_csv_env(monkeypatch):
    monkeypatch.setenv("ORBIT_WATCHED_DAGS", "a,b , c")
    assert Settings().watched_dag_ids == ["a", "b", "c"]


def test_orbit_dags_are_never_watched(monkeypatch):
    monkeypatch.setenv("ORBIT_WATCHED_DAGS", "retail_etl,orbit_remediation")
    assert "orbit_remediation" not in Settings().watched_dag_ids
