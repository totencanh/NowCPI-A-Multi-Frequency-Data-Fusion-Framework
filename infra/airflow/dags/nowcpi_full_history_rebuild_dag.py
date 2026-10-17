"""Manually rebuild Silver, Gold, and the current MIDAS nowcast from Bronze."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


def spark_submit_command(application: str, driver_port: int) -> str:
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
    dag_id="nowcpi_full_history_rebuild",
    description=(
        "Manually rebuild Silver and dbt Gold from the existing Bronze lake, "
        "then refresh the current MIDAS nowcast. Does not collect or publish data."
    ),
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nowcpi", "backfill", "silver", "gold"],
) as dag:
    wait_for_bronze = BashOperator(
        task_id="wait_for_bronze_catchup",
        bash_command=spark_submit_command("wait_for_bronze", 17178),
        execution_timeout=timedelta(minutes=35),
    )
    silver_cpi = BashOperator(
        task_id="silver_cpi_full_refresh",
        bash_command=spark_submit_command("cpi", 17180),
        env={"SILVER_FULL_REFRESH": "1"},
        append_env=True,
        execution_timeout=timedelta(hours=2),
    )
    silver_market = BashOperator(
        task_id="silver_market_full_refresh",
        bash_command=spark_submit_command("market", 17182),
        env={"SILVER_FULL_REFRESH": "1"},
        append_env=True,
        execution_timeout=timedelta(hours=2),
    )
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            "env -u PYTHONPATH /opt/dbt_venv/bin/dbt run "
            "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt "
            "--select path:models/staging"
        ),
        execution_timeout=timedelta(minutes=30),
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
        execution_timeout=timedelta(minutes=45),
    )
    midas_nowcast = BashOperator(
        task_id="midas_inflation_nowcast",
        bash_command="python -u /opt/airflow/forecasting/midas_nowcast.py",
        env={"TRINO_HOST": "trino", "TRINO_PORT": "8080", "TRINO_USER": "nowcpi"},
        append_env=True,
        execution_timeout=timedelta(minutes=45),
    )

    wait_for_bronze >> silver_cpi >> silver_market
    silver_market >> dbt_staging >> dbt_gold >> midas_nowcast
