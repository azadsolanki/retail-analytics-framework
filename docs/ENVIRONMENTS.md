# Multi-Environment Setup

This project supports four environments: dev, qa, prod, and ci.

## Environments

| Environment | Dataset | Purpose | Who Uses |
|-------------|---------|---------|----------|
| `dev` | `dbt_dev` | Local development | Individual developers |
| `qa` | `dbt_qa` | Integration testing | QA team, pre-release validation |
| `prod` | `dbt_prod` | Production data | End users, BI tools |
| `ci` | `dbt_ci_pr_*` | PR validation | GitHub Actions (ephemeral) |

## Usage

### Local Development

```bash
# Run against dev (default)
dbt run

# Explicitly specify dev
dbt run --target dev
```

### QA Testing

```bash
# Run against QA
dbt run --target qa
dbt test --target qa
```

### Production

```bash
# Run against prod (use with caution)
dbt run --target prod
```

## Workflow

```
Feature Branch → PR → main → QA → Production
     ↓           ↓      ↓      ↓        ↓
   dev/ci       ci    prod    qa      prod
```

### 1. Development

Developer works on feature branch:

```bash
git checkout -b feature/new-model
dbt run --select my_new_model --target dev
dbt test --select my_new_model --target dev
```

### 2. Pull Request

CI automatically:
- Creates ephemeral dataset `dbt_ci_pr_XX`
- Runs modified models (Slim CI)
- Runs tests
- Cleans up dataset

### 3. Merge to Main

On merge, CI:
- Runs full build against `prod`
- Uploads manifest to GCS
- Generates ERD

### 4. QA Validation (Optional)

For major releases:

```bash
dbt run --target qa
dbt test --target qa
```

QA team validates data quality before production promotion.

### 5. Production Deployment

Automated via Airflow DAGs running against `prod` target.

## Setup

### 1. Create Datasets

```bash
bq mk --dataset data-products-441119:dbt_dev
bq mk --dataset data-products-441119:dbt_qa
bq mk --dataset data-products-441119:dbt_prod
```

### 2. Configure Profile

Copy `profiles.yml` to `~/.dbt/profiles.yml`:

```bash
cp profiles.yml ~/.dbt/profiles.yml
```

### 3. Verify Connection

```bash
dbt debug --target dev
dbt debug --target qa
dbt debug --target prod
```

## Environment Variables

For CI/CD, these variables control behavior:

| Variable | Description |
|----------|-------------|
| `DBT_TARGET` | Target environment (dev/qa/prod/ci) |
| `DBT_DATASET` | Override dataset name |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account key |

## Best Practices

1. **Never run untested code against prod** - Always test in dev/qa first
2. **Use Slim CI** - Only build modified models in PRs
3. **Review before merge** - PRs require approval before merging to main
4. **Monitor prod** - Use Airflow DAGs for scheduled runs and freshness checks
5. **Rollback plan** - Keep previous manifest for quick rollback if needed
