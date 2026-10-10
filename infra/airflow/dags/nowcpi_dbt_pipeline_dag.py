"""NowCPI dbt transformations: staging -> subject-specific Gold tables.

This DAG is manually triggered after Silver refreshes. It transforms tables
already registered in Trino; it does not fetch source data or run Spark jobs.
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
    description="Build subject-specific dashboard tables in the NowCPI Gold layer",
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
        env={"PYTHONPATH": ""},
        append_env=True,
    )

    staging = DbtTaskGroup(
        group_id="staging",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"env": {"PYTHONPATH": ""}},
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/staging"],
        ),
    )

    gold = DbtTaskGroup(
        group_id="gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"env": {"PYTHONPATH": ""}},
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/gold"],
        ),
    )

    drop_legacy_gold = BashOperator(
        task_id="drop_legacy_gold_tables",
        bash_command=(
            f"env -u PYTHONPATH {DBT_EXECUTABLE} run-operation "
            "drop_legacy_gold_tables "
            f"--project-dir {DBT_PROJECT_PATH} --profiles-dir {DBT_PROJECT_PATH}"
        ),
        env={"PYTHONPATH": ""},
        append_env=True,
    )

    start >> dbt_deps >> staging >> drop_legacy_gold >> gold >> end
