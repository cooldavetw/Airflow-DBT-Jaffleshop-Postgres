from airflow.datasets import Dataset
from datetime import datetime
import os

from cosmos.providers.dbt.dag import DbtDag


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_EXECUTABLE_PATH = os.environ.get("DBT_EXECUTABLE_PATH", "dbt")

jaffle_shop = DbtDag(
    dag_id="jaffle_shop",
    dbt_project_name="jaffle_shop",
    start_date=datetime(2023, 1, 1),
    schedule=[Dataset("SEED://JAFFLE_SHOP")],
    conn_id="postgres",
    dbt_args={
        "schema": "public",
        "dbt_executable_path": DBT_EXECUTABLE_PATH,
    },
)

jaffle_shop
