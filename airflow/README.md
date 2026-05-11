# Airflow DAGs

Production orchestration for dbt using Apache Airflow.

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `dbt_production` | Daily 6 AM | Full pipeline: staging → marts → reports |
| `dbt_incremental` | Hourly | Only incremental models |
| `dbt_source_freshness` | Every 30 min | Monitor source data freshness |

## Setup

### 1. Install Airflow with dbt

```bash
uv pip install apache-airflow apache-airflow-providers-slack dbt-bigquery
```

### 2. Configure Connections

In Airflow UI → Admin → Connections:

**Google Cloud Connection:**
- Conn ID: `google_cloud_default`
- Conn Type: Google Cloud
- Keyfile JSON: Your service account key

**Slack Webhook:**
- Conn ID: `slack_webhook`
- Conn Type: Slack Webhook
- Host: Your Slack webhook URL

### 3. Set Environment Variables

```bash
export DBT_PROJECT_DIR="/opt/dbt/retail-analytics-framework"
export DBT_PROFILES_DIR="/opt/dbt/.dbt"
```

### 4. Deploy DAGs

Copy DAGs to your Airflow DAGs folder:

```bash
cp airflow/dags/*.py $AIRFLOW_HOME/dags/
```

## DAG Details

### dbt_production

Daily full refresh pipeline:

```
dbt_deps → dbt_staging → test_staging → dbt_intermediate → dbt_marts → test_marts → dbt_reports → dbt_docs → notify_success
```

- Runs at 6 AM daily
- Tests after each layer
- Slack notification on success/failure
- 2 retries with 5-minute delay

### dbt_incremental

Hourly incremental updates:

```
dbt_incremental → test_incremental
```

- Only runs models with `materialized='incremental'`
- Processes only new/changed data
- Lightweight for frequent runs

### dbt_source_freshness

Source monitoring:

```
check_freshness → alert_stale
```

- Runs every 30 minutes
- Outputs freshness report to JSON
- Alerts team via Slack

## Customization

### Change Schedule

Edit `schedule_interval` in DAG definition:

```python
schedule_interval="0 6 * * *"     # Daily at 6 AM
schedule_interval="0 * * * *"     # Hourly
schedule_interval="*/30 * * * *"  # Every 30 minutes
schedule_interval="0 6 * * 1"     # Weekly Monday 6 AM
```

### Add SLA Monitoring

```python
from airflow.operators.bash import BashOperator
from datetime import timedelta

dbt_marts = BashOperator(
    task_id="dbt_marts",
    bash_command="...",
    sla=timedelta(hours=1),  # Alert if takes > 1 hour
)
```

### Add Data Quality Checks

```python
from airflow.operators.python import BranchPythonOperator

def check_row_counts(**context):
    # Query BigQuery for row counts
    # Return 'continue' or 'alert' based on thresholds
    pass

quality_check = BranchPythonOperator(
    task_id="quality_check",
    python_callable=check_row_counts,
)
```

## Monitoring

### Airflow UI

Access at `http://localhost:8080` (default)

- View DAG runs
- Check task logs
- Monitor SLAs
- Trigger manual runs

### Troubleshoot

- Local Airflow Dag can fail due to:
  - Not having dbt profile. Fix: Ensure dbt profile is present
  - Missing GCP Authentication. Fix: Run  `gcloud auth application-default login`
  - Missing dbt deps step. Fix: Run dbt deps dag first.  


### Slack Alerts

Configure webhook in Airflow connections for:
- Task failures
- SLA misses
- Source freshness warnings
](https://docs.basedpyright.com/v1.39.3/configuration/config-files/#reportMissingImports)
