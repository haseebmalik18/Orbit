from fastapi.testclient import TestClient

from orbit.api.app import PANEL, orbit_api

client = TestClient(orbit_api)


def test_panel_is_served():
    response = client.get("/ui/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_panel_shell_needs_no_credentials():
    """The shell is inert markup; its data calls are what carry auth."""
    assert client.get("/ui/").status_code == 200


def test_panel_file_exists_and_is_self_contained():
    html = PANEL.read_text()
    assert "<title>Orbit</title>" in html
    # no external fetches — a CDN dependency would break in an offline demo
    assert "http://" not in html.replace("http://www.w3.org", "")
    assert "cdn." not in html


def test_panel_calls_the_api_relatively():
    """Absolute paths break when the plugin is mounted under a prefix."""
    html = PANEL.read_text()
    assert '"../api"' in html or "'../api'" in html or '"../api" +' in html


def test_panel_covers_every_required_check():
    from orbit.contracts import REQUIRED_CHECKS

    html = PANEL.read_text()
    for check in REQUIRED_CHECKS:
        assert f'"{check}"' in html, check


def test_panel_styles_both_themes():
    html = PANEL.read_text()
    assert "prefers-color-scheme: dark" in html
    assert '[data-theme="dark"]' in html


def test_status_is_never_colour_alone():
    """Status colours ship with a glyph and a label, never colour by itself."""
    html = PANEL.read_text()
    assert "GLYPH" in html
    assert "aria-hidden" in html


def test_panel_reads_the_task_coordinates_from_the_url():
    """Airflow templates them into the href; the panel picks them up here."""
    html = PANEL.read_text()
    assert "URLSearchParams" in html
    for param in ("dag_id", "task_id", "run_id"):
        assert f'"{param}"' in html, param


def test_scoped_mode_filters_the_incident_query():
    """A task panel showing another task's incident is worse than showing none."""
    html = PANEL.read_text()
    assert "/incidents?" in html


def test_scoped_mode_has_its_own_empty_state():
    """Most tasks never have an incident. That is normal, not an error."""
    html = PANEL.read_text()
    assert "No Orbit incident" in html


def test_the_replay_count_is_drawn_not_just_counted():
    """How many recorded runs survived the patch is the argument Orbit makes.
    It comes off the regression check's own detail, never a constant."""
    html = PANEL.read_text()
    assert "replayStrip" in html
    assert "d.total" in html and "d.passed" in html


def test_the_replay_strip_is_labelled_for_screen_readers():
    html = PANEL.read_text()
    assert 'role="img"' in html and "aria-label" in html


def test_the_verdict_names_what_was_checked():
    """"Proved" never said proved what, and overclaims what four checks can
    establish. The panel speaks the same word the store does."""
    html = PANEL.read_text()
    assert "Fix verified" in html and "Not verified" in html
    assert "Proved" not in html


def test_durations_are_drawn_to_a_shared_scale():
    """Bars scaled per-row instead of against the slowest check would make a
    0.0s scope check look as expensive as a 25s replay."""
    html = PANEL.read_text()
    assert "longest" in html


def test_record_column_is_width_capped():
    """Uncapped, a findings row strands its verdict a screen from its label."""
    html = PANEL.read_text()
    assert ".wrap" in html and "max-width" in html


def test_poll_interval_meets_the_two_second_freshness_bar():
    import re

    html = PANEL.read_text()
    intervals = [int(m) for m in re.findall(r"setInterval\([^,]+,\s*(\d+)\)", html)]
    assert intervals, "panel does not poll at all"
    assert max(intervals) <= 2000, intervals
