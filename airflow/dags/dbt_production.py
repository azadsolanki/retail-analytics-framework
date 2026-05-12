"""
dbt Production DAG

Runs dbt models on a daily schedule with proper dependency management.
"""

import os
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

# Configuration - override with environment variables for Cloud Composer
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/home/airflow/gcs/dags/dbt")
DBT_TARGET = os.getenv("DBT_TARGET", "prod")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_production",
    default_args=default_args,
    description="Daily dbt production run",
    schedule="0 6 * * *",  # 6 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "production"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --target {DBT_TARGET}",
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select tag:staging --target {DBT_TARGET}",
    )

    test_staging = BashOperator(
        task_id="test_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select tag:staging --target {DBT_TARGET}",
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select tag:intermediate --target {DBT_TARGET}",
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select tag:marts --target {DBT_TARGET}",
    )

    test_marts = BashOperator(
        task_id="test_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select tag:marts --target {DBT_TARGET}",
    )

    dbt_reports = BashOperator(
        task_id="dbt_reports",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select tag:reports --target {DBT_TARGET}",
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --target {DBT_TARGET}",
    )

    (
        dbt_deps
        >> dbt_staging
        >> test_staging
        >> dbt_intermediate
        >> dbt_marts
        >> test_marts
        >> dbt_reports
        >> dbt_docs
    )
