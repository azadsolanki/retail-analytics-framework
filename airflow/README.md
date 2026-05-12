# Airflow DAGs

Production orchestration for dbt using Apache Airflow.

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `dbt_production` | Daily 6 AM | Full pipeline: staging → marts → reports |
| `dbt_incremental` | Hourly | Only incremental models |
| `dbt_source_freshness` | Every 30 min | Monitor source data freshness |

## Configuration

DAGs use environment variables for flexibility:

| Variable | Default | Description |
|----------|---------|-------------|
| `DBT_PROJECT_DIR` | `/home/airflow/gcs/dags/dbt` | Path to dbt project |
| `DBT_TARGET` | `prod` | dbt target to use |

## Local Setup

### 1. Install Airflow

```bash
uv pip install apache-airflow
```

### 2. Initialize Database

```bash
airflow db migrate
```

### 3. Configure DAGs Folder

Edit `~/airflow/airflow.cfg`:

```
dags_folder = /path/to/retail-analytics-framework/airflow/dags
load_examples = False
```

### 4. Set Environment Variables

```bash
export DBT_PROJECT_DIR="/path/to/retail-analytics-framework"
export DBT_TARGET="dev"
```

### 5. Start Airflow

```bash
# Terminal 1: API server
airflow api-server

# Terminal 2: Scheduler
airflow scheduler
```

### 6. Access UI

Open http://localhost:8080

## Cloud Composer Deployment

### 1. Set Environment Variables

In Cloud Composer → Environment Variables:

```
DBT_PROJECT_DIR = /home/airflow/gcs/dags/dbt
DBT_TARGET = prod
```

### 2. Upload DAGs

```bash
gsutil cp airflow/dags/*.py gs://your-composer-bucket/dags/
```

### 3. Upload dbt Project

```bash
gsutil -m cp -r models macros seeds snapshots dbt_project.yml packages.yml gs://your-composer-bucket/dags/dbt/
```

## DAG Dependencies

```
dbt_production:
  dbt_deps → dbt_staging → test_staging → dbt_intermediate → dbt_marts → test_marts → dbt_reports → dbt_docs

dbt_incremental:
  dbt_incremental → test_incremental

dbt_source_freshness:
  check_freshness
```

## Adding Slack Notifications

Install provider:

```bash
uv pip install apache-airflow-providers-slack
```

Add to DAG:

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

notify = SlackWebhookOperator(
    task_id="notify",
    slack_webhook_conn_id="slack_webhook",
    message="dbt run complete",
)
```

## Troubleshooting

Local Airflow DAGs can fail due to:

| Issue | Fix |
|-------|-----|
| Missing dbt profile | Ensure `~/.dbt/profiles.yml` contains `thelook_analytics` profile |
| Missing GCP auth | Run `gcloud auth application-default login` |
| Missing dbt packages | Run `dbt_deps` task first or `dbt deps` manually |
| DAGs not showing | Check `dags_folder` in `~/airflow/airflow.cfg` and run `airflow dags reserialize` |
