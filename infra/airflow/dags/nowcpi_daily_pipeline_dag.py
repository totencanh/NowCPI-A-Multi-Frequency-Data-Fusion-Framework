"""Daily NowCPI pipeline from source collection through dbt marts."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


def spark_submit_command(application: str, driver_port: int) -> str:
    """Submit one batch application from Airflow's Spark client to the cluster."""
    return f"""
"${{SPARK_HOME}}/bin/spark-submit" \\
  --master spark://spark-master:7077 \\
  --deploy-mode client \\
  --conf spark.driver.host=airflow-scheduler \\
  --conf spark.driver.bindAddress=0.0.0.0 \\
  --conf spark.driver.port={driver_port} \\
  --conf spark.blockManager.port={driver_port + 1} \\
  --conf spark.jars.packages=io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.791 \\
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.endpoint="$MINIO_ENDPOINT" \\
  --conf spark.hadoop.fs.s3a.access.key="$MINIO_ROOT_USER" \\
  --conf spark.hadoop.fs.s3a.secret.key="$MINIO_ROOT_PASSWORD" \\
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \\
  --conf spark.hadoop.fs.s3a.path.style.access=true \\
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \\
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \\
  /opt/spark/app/spark/silver/{application}.py
""".strip()


with DAG(
    dag_id="nowcpi_daily_pipeline",
    description="Collect data, publish Kafka events, process Bronze and Silver, then build dbt marts.",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nowcpi", "ingestion", "bronze", "silver", "spark"],
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
    collect_iip = BashOperator(
        task_id="collect_iip",
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

    wait_for_bronze = BashOperator(
        task_id="wait_for_bronze_catchup",
        bash_command=spark_submit_command("wait_for_bronze", 17178),
        execution_timeout=timedelta(minutes=35),
    )
    silver_cpi = BashOperator(
        task_id="silver_cpi_sources",
        bash_command=spark_submit_command("cpi", 17180),
    )
    silver_market = BashOperator(
        task_id="silver_market_sources",
        bash_command=spark_submit_command("market", 17182),
    )
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            "env -u PYTHONPATH /opt/dbt_venv/bin/dbt run "
            "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt "
            "--select path:models/staging"
        ),
    )
    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=(
            "env -u PYTHONPATH /opt/dbt_venv/bin/dbt run-operation "
            "drop_legacy_gold_tables "
            "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt && "
            "env -u PYTHONPATH /opt/dbt_venv/bin/dbt run "
            "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt "
            "--select path:models/gold"
        ),
    )

    [
        collect_cpi,
        collect_cpi_components,
        collect_iip,
        collect_brent,
        collect_vn_fuel,
        collect_usd_vnd,
    ] >> publish_to_kafka
    publish_to_kafka >> wait_for_bronze >> silver_cpi >> silver_market
    silver_market >> dbt_staging >> dbt_gold
