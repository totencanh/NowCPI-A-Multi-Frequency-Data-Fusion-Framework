"""Daily NowCPI source collection and Bronze ingestion."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def run_spark_bronze(script_name: str) -> None:
    """Run a Bronze Spark job inside the existing Spark master container."""
    import docker

    client = docker.from_env()
    try:
        spark_master = client.containers.get("nowcpi-spark-master")
        command = [
            "/opt/bitnami/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            f"/opt/spark/app/spark/bronze/{script_name}",
        ]
        result = spark_master.exec_run(command, stdout=True, stderr=True)
        output = result.output
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        print(output)
        if result.exit_code != 0:
            raise RuntimeError(
                f"Spark Bronze job {script_name} failed "
                f"with exit code {result.exit_code}:\n{output}"
            )
    finally:
        client.close()


with DAG(
    dag_id="nowcpi_daily_ingestion",
    description="Collect daily source batches and upsert them into Bronze",
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

    bronze_cpi = PythonOperator(
        task_id="bronze_cpi",
        python_callable=run_spark_bronze,
        op_kwargs={"script_name": "cpi.py"},
    )
    bronze_market = PythonOperator(
        task_id="bronze_market",
        python_callable=run_spark_bronze,
        op_kwargs={"script_name": "market.py"},
    )

    [collect_cpi, collect_cpi_components, collect_ppi_iip] >> bronze_cpi
    [collect_brent, collect_vn_fuel, collect_usd_vnd] >> bronze_market
