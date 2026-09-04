"""The registration table is read by Airflow at import time. A bad key or an
unsupported placeholder does not raise — the UI just drops the entry."""

import re

from plugins.orbit_plugin import OrbitPlugin

# Airflow substitutes exactly these into an external view href, URL-encoding
# each. Read out of airflow/ui/dist: l.replaceAll(`{DAG_ID}`, encodeURI...).
SUPPORTED_PLACEHOLDERS = {"{DAG_ID}", "{RUN_ID}", "{TASK_ID}", "{MAP_INDEX}"}


def _view(destination):
    matches = [v for v in OrbitPlugin.external_views if v["destination"] == destination]
    assert len(matches) == 1, f"expected 1 {destination} view, got {len(matches)}"
    return matches[0]


def test_nav_view_is_registered():
    assert _view("nav")["href"] == "/orbit/ui/"


def test_task_instance_view_is_registered():
    """The tab bar on a task-instance page filters on "task_instance". Under
    "task" alone no Orbit tab renders there at all — which is the page an
    operator actually lands on from a red square."""
    assert _view("task_instance")["href"].startswith("/orbit/ui/")


def test_task_instance_view_carries_all_three_coordinates():
    href = _view("task_instance")["href"]
    for placeholder in ("{DAG_ID}", "{TASK_ID}", "{RUN_ID}"):
        assert placeholder in href, placeholder


def test_task_view_omits_the_run_it_cannot_supply():
    """The across-runs task page has no run in scope, so asking for {RUN_ID}
    there would send an unsubstituted literal to the panel."""
    assert "{RUN_ID}" not in _view("task")["href"]


def test_views_use_only_placeholders_airflow_substitutes():
    """An unrecognised token is left verbatim and reaches the panel as the
    literal string '{DAG_ID}', which then matches no incident."""
    for view in OrbitPlugin.external_views:
        found = set(re.findall(r"\{[A-Z_]+\}", view["href"]))
        assert found <= SUPPORTED_PLACEHOLDERS, found - SUPPORTED_PLACEHOLDERS


def test_every_view_is_named():
    for view in OrbitPlugin.external_views:
        assert view.get("name")


def test_the_api_is_still_mounted_under_orbit():
    """The panel fetches ../api relative to /orbit/ui/, so the prefix and the
    panel route have to stay in agreement."""
    assert OrbitPlugin.fastapi_apps[0]["url_prefix"] == "/orbit"
