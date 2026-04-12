# CI/CD Pipeline

## Overview

GitHub Actions pipeline for dbt with Slim CI support. Manifests are stored in GCS, enabling faster builds in both CI and local development.

## Workflow

| Trigger | What Runs |
|---------|-----------|
| Push to `feature/**` | compile, full build |
| Pull Request to `main` | compile, slim build, docs |
| Merge to `main` | compile, full build, upload manifest, ERD |

## Slim CI

Slim CI only builds modified models and their downstream dependencies by comparing against the production manifest.

```bash
# CI runs this on PRs:
dbt build --select state:modified+ --defer --state ./prod-manifest
```

The manifest is stored in GCS:
```
gs://data-products-441119-dbt-artifacts/manifests/prod/manifest.json
```

## Local Development

Developers can use Slim CI locally for faster iteration.

### Setup (one time)

```bash
gcloud auth login
./scripts/dev-setup.sh
```

### Daily workflow

```bash
# See what changed
./scripts/list-modified.sh

# Build only changed models
./scripts/slim-build.sh

# Or run dbt directly
dbt build --select state:modified+ --defer --state ./prod-manifest
```

### Refresh manifest

Run `./scripts/dev-setup.sh` again after others merge changes to main.

## Dataset Naming

| Event | Dataset |
|-------|---------|
| Pull Request | `dbt_ci_pr_<number>` |
| Push | `dbt_ci_run_<id>` |

Datasets are deleted automatically after builds complete.

## ERD Generation

ERD is generated on merge to main using relationship tests defined in schema YAML files. Only `dim_*`, `fct_*`, and `rpt_*` models are included.

To add relationships, define tests in your schema YAML:

```yaml
columns:
  - name: user_id
    tests:
      - relationships:
          to: ref('dim_users')
          field: user_id
```

## Secrets Required

| Secret | Description |
|--------|-------------|
| `GCP_SERVICE_ACCOUNT_KEY` | Service account JSON with BigQuery and GCS access |

## Troubleshooting

**Slim CI runs full build**: No manifest in GCS yet. Merge to main first.

**Can't download manifest locally**: Run `gcloud auth login` and ensure you have GCS access.

**ERD missing relationships**: Add `relationships` tests to your schema YAML files.
