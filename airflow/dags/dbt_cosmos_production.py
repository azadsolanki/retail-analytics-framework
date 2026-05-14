"""
dbt Production DAG using Astronomer Cosmos

Cosmos automatically creates one Airflow task per dbt model,
preserving dbt's DAG structure in Airflow UI.

Benefits:
- Model-level visibility and retries
- dbt lineage visible in Airflow
- Tests run immediately after each model
- No custom code needed for dependencies
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# Configuration
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/home/airflow/gcs/dags/dbt"))
DBT_TARGET = os.getenv("DBT_TARGET", "prod")
GCP_PROJECT = os.getenv("GCP_PROJECT", "data-products-441119")
GCP_KEYFILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/gcp-key.json")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Profile configuration for BigQuery
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

# Create the Cosmos DAG
dbt_production = DbtDag(
    dag_id="dbt_cosmos_production",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_DIR,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.getenv('DBT_VENV_PATH', '/usr/local/airflow/dbt_venv')}/bin/dbt",
    ),
    render_config=RenderConfig(
        exclude=["snap_products"],  # Exclude snapshots
    ),
    schedule="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "cosmos", "production"],
)
