"""
dbt DAG with Task Groups using Astronomer Cosmos

Organizes dbt models into Airflow Task Groups by layer:
- staging
- intermediate  
- marts
- reports

This provides better visual organization in Airflow UI.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# Configuration
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/home/airflow/gcs/dags/dbt"))
DBT_TARGET = os.getenv("DBT_TARGET", "prod")
GCP_PROJECT = os.getenv("GCP_PROJECT", "data-products-441119")
GCP_KEYFILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/gcp-key.json")
DBT_EXECUTABLE = f"{os.getenv('DBT_VENV_PATH', '/usr/local/airflow/dbt_venv')}/bin/dbt"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

profile_config = ProfileConfig(
    profile_name="thelook_analytics",
    target_name=DBT_TARGET,
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default",
        profile_args={
            "project": GCP_PROJECT,
            "dataset": f"dbt_{DBT_TARGET}",
            "threads": 4,
            "keyfile": GCP_KEYFILE,
            "location": "US",
        },
    ),
)

with DAG(
    dag_id="dbt_cosmos_layered",
    default_args=default_args,
    description="dbt pipeline organized by layer using Cosmos Task Groups",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "cosmos", "layered"],
) as dag:

    # Staging layer
    staging = DbtTaskGroup(
        group_id="staging",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(select=["path:models/staging"]),
    )

    # Intermediate layer
    intermediate = DbtTaskGroup(
        group_id="intermediate",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(select=["path:models/intermediate"]),
    )

    # Marts layer
    marts = DbtTaskGroup(
        group_id="marts",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(select=["path:models/marts"]),
    )

    # Reports layer
    reports = DbtTaskGroup(
        group_id="reports",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(select=["path:models/reports"]),
    )

    # Define layer dependencies
    staging >> intermediate >> marts >> reports
