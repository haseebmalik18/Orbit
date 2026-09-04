from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from orbit import listener
from orbit.api.app import orbit_api


class OrbitPlugin(AirflowPlugin):
    name = "orbit"
    listeners = [listener]
    fastapi_apps = [
        {
            "app": orbit_api,
            "url_prefix": "/orbit",
            "name": "Orbit API",
        }
    ]
    external_views = [
        {
            "name": "Orbit",
            "href": "/orbit/ui/",
            "destination": "nav",
            "category": "Browse",
        }
    ]
