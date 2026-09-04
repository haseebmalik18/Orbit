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
