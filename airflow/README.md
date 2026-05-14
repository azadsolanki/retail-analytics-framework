# Airflow DAGs

Production orchestration for dbt using Apache Airflow.

## DAGs

### Standard DAGs (BashOperator)

| DAG | Schedule | Description |
|-----|----------|-------------|
| `dbt_production` | Daily 6 AM | Full pipeline: staging → marts → reports |
| `dbt_incremental` | Hourly | Only incremental models |
| `dbt_source_freshness` | Every 30 min | Monitor source data freshness |

### Cosmos DAGs (Recommended)

| DAG | Schedule | Description |
|-----|----------|-------------|
| `dbt_cosmos_production` | Daily 6 AM | Auto-generated tasks per dbt model |
| `dbt_cosmos_layered` | Daily 6 AM | Task groups organized by layer |

**Why Cosmos?**
- Each dbt model becomes an Airflow task
- dbt lineage visible in Airflow UI
- Model-level retries and monitoring
- Tests run after each model automatically

## Configuration

DAGs use environment variables for flexibility:

| Variable | Default | Description |
|----------|---------|-------------|
| `DBT_PROJECT_DIR` | `/home/airflow/gcs/dags/dbt` | Path to dbt project |
| `DBT_TARGET` | `prod` | dbt target to use |
| `GCP_PROJECT` | `data-products-441119` | GCP project ID |
| `DBT_VENV_PATH` | `/usr/local/airflow/dbt_venv` | Path to dbt virtual env |

## Local Setup

### 1. Install Airflow and Cosmos

```bash
uv pip install apache-airflow astronomer-cosmos dbt-bigquery
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
export DBT_VENV_PATH="/path/to/.venv"
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
GCP_PROJECT = data-products-441119
```

### 2. Install Cosmos

Add to `requirements.txt` in Composer:

```
astronomer-cosmos[dbt-bigquery]
```

### 3. Upload DAGs

```bash
gsutil cp airflow/dags/*.py gs://your-composer-bucket/dags/
```

### 4. Upload dbt Project

```bash
gsutil -m cp -r models macros seeds snapshots dbt_project.yml packages.yml gs://your-composer-bucket/dags/dbt/
```

## DAG Dependencies

### Standard DAGs
```
dbt_production:
  dbt_deps → dbt_staging → test_staging → dbt_intermediate → dbt_marts → test_marts → dbt_reports → dbt_docs
```

### Cosmos DAGs
```
dbt_cosmos_layered:
  [staging group] → [intermediate group] → [marts group] → [reports group]
  
Each group contains individual tasks per model with tests.
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
| Cosmos import error | Install with `uv pip install astronomer-cosmos[dbt-bigquery]` |
