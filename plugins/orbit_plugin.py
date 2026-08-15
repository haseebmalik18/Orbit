from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from orbit.api.app import orbit_api


class OrbitPlugin(AirflowPlugin):
    name = "orbit"
    fastapi_apps = [
        {
            "app": orbit_api,
            "url_prefix": "/orbit",
            "name": "Orbit API",
        }
    ]
