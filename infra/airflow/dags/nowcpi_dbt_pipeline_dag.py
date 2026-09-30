"""NowCPI dbt transformations: staging -> intermediate -> mart.

This DAG is manually triggered while source cadence and the Spark Silver layer
are being finalized. It transforms tables already registered in Trino; it does
not fetch source data or run Spark jobs.
"""

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

DBT_PROJECT_PATH = Path("/opt/airflow/dbt")
DBT_EXECUTABLE = "/opt/dbt_venv/bin/dbt"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="nowcpi",
    install_dbt_deps=False,
)

profile_config = ProfileConfig(
    profile_name="nowcpi",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE,
)

with DAG(
    dag_id="nowcpi_dbt_pipeline",
    description="Transform available NowCPI lakehouse tables through dbt layers",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["nowcpi", "dbt", "inflation"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_PATH} && {DBT_EXECUTABLE} deps",
    )

    staging = DbtTaskGroup(
        group_id="staging",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/staging"],
        ),
    )

    intermediate = DbtTaskGroup(
        group_id="intermediate",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/intermediate"],
        ),
    )

    mart = DbtTaskGroup(
        group_id="mart",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/mart"],
        ),
    )

    start >> dbt_deps >> staging >> intermediate >> mart >> end
