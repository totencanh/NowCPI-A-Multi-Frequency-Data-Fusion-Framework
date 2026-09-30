FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl git && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER airflow
COPY requirements.txt /tmp/airflow-requirements.txt
RUN uv pip install --target /home/airflow/.local/lib/python3.11/site-packages \
    --no-cache -r /tmp/airflow-requirements.txt

RUN python -m venv /opt/dbt_venv
COPY requirements-dbt.txt /tmp/dbt-requirements.txt
RUN VIRTUAL_ENV=/opt/dbt_venv uv pip install --no-cache -r /tmp/dbt-requirements.txt
