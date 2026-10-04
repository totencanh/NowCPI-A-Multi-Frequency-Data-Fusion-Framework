"""Daily NowCPI source collection and Kafka publication."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


with DAG(
    dag_id="nowcpi_daily_ingestion",
    description="Collect source batches and publish new events to Kafka; Spark continuously consumes them into Bronze",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nowcpi", "ingestion", "bronze"],
) as dag:
    # Source collectors are independent and can run at the same time.
    collect_cpi = BashOperator(
        task_id="collect_cpi",
        bash_command="python -u /opt/airflow/ingestion/cpi/cpi.py",
    )
    collect_cpi_components = BashOperator(
        task_id="collect_cpi_components",
        bash_command="python -u /opt/airflow/ingestion/cpi_components/cpi_components.py",
    )
    collect_ppi_iip = BashOperator(
        task_id="collect_ppi_iip",
        bash_command="python -u /opt/airflow/ingestion/ppi_iip/ppi_iip.py",
    )
    collect_brent = BashOperator(
        task_id="collect_brent",
        bash_command="python -u /opt/airflow/ingestion/oil/brent.py",
    )
    collect_vn_fuel = BashOperator(
        task_id="collect_vn_fuel",
        bash_command="python -u /opt/airflow/ingestion/oil/vn.py",
    )
    collect_usd_vnd = BashOperator(
        task_id="collect_usd_vnd",
        bash_command="python -u /opt/airflow/ingestion/usd/usd_vnd.py",
    )

    publish_to_kafka = BashOperator(
        task_id="publish_to_kafka",
        bash_command="python -u /opt/airflow/kafka/producer.py",
        env={
            "KAFKA_BROKER": "kafka:9092",
            "RAW_DATA_DIR": "/opt/airflow/data/raw",
            "KAFKA_PUBLISHED_STATE": "/opt/airflow/kafka-state/published_batches.json",
        },
        append_env=True,
        # Publish any batches produced by successful collectors even if a different source failed.
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [
        collect_cpi,
        collect_cpi_components,
        collect_ppi_iip,
        collect_brent,
        collect_vn_fuel,
        collect_usd_vnd,
    ] >> publish_to_kafka
