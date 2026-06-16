from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from pendulum import datetime
import os

from cosmos.providers.dbt.core.operators import (
    DbtDepsOperator,
    DbtRunOperationOperator,
    DbtSeedOperator,
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_EXECUTABLE_PATH = os.environ.get("DBT_EXECUTABLE_PATH", "dbt")

with DAG(
    dag_id="import-seeds",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    project_seeds = [
        {
            "project": "jaffle_shop",
            "seeds": ["raw_customers", "raw_payments", "raw_orders"],
        }
    ]

    deps_install = DbtDepsOperator(
        task_id="jaffle_shop_install_deps",
        project_dir=f"{AIRFLOW_HOME}/dbt/jaffle_shop",
        schema="public",
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        conn_id="postgres",
    )

    with TaskGroup(group_id="drop_seeds_if_exist") as drop_seeds:
        for project in project_seeds:
            for seed in project["seeds"]:
                DbtRunOperationOperator(
                    task_id=f"drop_{seed}_if_exists",
                    macro_name="drop_table",
                    args={"table_name": seed},
                    project_dir=f"{AIRFLOW_HOME}/dbt/{project['project']}",
                    schema="public",
                    dbt_executable_path=DBT_EXECUTABLE_PATH,
                    conn_id="postgres",
                )

    create_seeds = DbtSeedOperator(
        task_id="jaffle_shop_seed",
        project_dir=f"{AIRFLOW_HOME}/dbt/jaffle_shop",
        schema="public",
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        conn_id="postgres",
        outlets=[Dataset("SEED://JAFFLE_SHOP")],
    )

    deps_install >> drop_seeds >> create_seeds
