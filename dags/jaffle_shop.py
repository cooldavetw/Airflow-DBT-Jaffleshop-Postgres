from airflow.datasets import Dataset
from datetime import datetime
import os
from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping



AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_EXECUTABLE_PATH = os.environ.get("DBT_EXECUTABLE_PATH", "dbt")
DBT_PROJECT_DIR = f"{AIRFLOW_HOME}/dbt/jaffle_shop"

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres",
        profile_args={"schema": "public"},
    ),
)

jaffle_shop = DbtDag(
    dag_id="jaffle_shop",
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    profile_config=profile_config,
    start_date=datetime(2023, 1, 1),
    schedule=[Dataset("SEED://JAFFLE_SHOP")],
    operator_args={
        "dbt_executable_path": DBT_EXECUTABLE_PATH,
    },
)

jaffle_shop
