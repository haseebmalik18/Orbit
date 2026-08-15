FROM astrocrpublic.azurecr.io/runtime:3.3-2

# Orbit's library code lives in src/ and is imported by dags/ and plugins/.
ENV PYTHONPATH=/usr/local/airflow/src:$PYTHONPATH
