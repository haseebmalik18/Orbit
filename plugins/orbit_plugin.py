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
        # panel opens already scoped to what is on screen. The tab bar on a
        # task-instance page filters on "task_instance"; "task" is the
        # across-runs page, where {RUN_ID} is left unsubstituted and the panel
        # falls back to matching on dag and task alone.
        {
            "name": "Orbit",
            "href": "/orbit/ui/?dag_id={DAG_ID}&task_id={TASK_ID}&run_id={RUN_ID}",
            "destination": "task_instance",
        },
        {
            "name": "Orbit",
            "href": "/orbit/ui/?dag_id={DAG_ID}&task_id={TASK_ID}",
            "destination": "task",
        },
    ]
