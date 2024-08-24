import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres",  # Make sure this Airflow connection is set up
        profile_args={
            "schema": "marketing",
            # You can add other PostgreSQL-specific args here if needed
        },
    )
)

dbt_postgres_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/airflow_dbt_project"),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_postgres_dag",
)