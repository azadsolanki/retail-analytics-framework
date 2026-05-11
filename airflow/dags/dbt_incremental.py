"""
dbt Incremental DAG

Runs incremental models hourly for near real-time updates.
"""

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

# Configuration
DBT_PROJECT_DIR = "/home/azad/VajraDev/retail-analytics-framework"
DBT_TARGET = "prod"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dbt_incremental",
    default_args=default_args,
    description="Hourly incremental dbt run",
    schedule="0 * * * *",  # Every hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "incremental"],
) as dag:
    dbt_incremental = BashOperator(
        task_id="dbt_incremental",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select config.materialized:incremental --target {DBT_TARGET}",
    )

    test_incremental = BashOperator(
        task_id="test_incremental",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select config.materialized:incremental --target {DBT_TARGET}",
    )

    dbt_incremental >> test_incremental
