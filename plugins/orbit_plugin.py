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
        },
        # Airflow substitutes the braced tokens and URL-encodes each, so the
        # panel opens already scoped to the task instance being viewed.
        {
            "name": "Orbit",
            "href": "/orbit/ui/?dag_id={DAG_ID}&task_id={TASK_ID}&run_id={RUN_ID}",
            "destination": "task",
        },
    ]
