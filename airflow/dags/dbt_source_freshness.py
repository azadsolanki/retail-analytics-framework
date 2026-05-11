"""
dbt Source Freshness DAG

Monitors source data freshness.
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
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_source_freshness",
    default_args=default_args,
    description="Monitor source data freshness",
    schedule="*/30 * * * *",  # Every 30 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "freshness", "monitoring"],
) as dag:
    check_freshness = BashOperator(
        task_id="check_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness --target {DBT_TARGET}",
    )
