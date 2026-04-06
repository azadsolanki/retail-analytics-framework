# CI/CD Pipeline Documentation

## Overview

This repository uses GitHub Actions to automate dbt testing, documentation, and ERD generation.

## Workflow Summary

| Trigger | Jobs That Run |
|---------|---------------|
| Push to `feature/**` | compile → build |
| Pull Request to `main` | compile → build → docs |
| Merge to `main` | compile → build → ERD generation |

## Jobs

### 🔍 Compile
- Validates dbt syntax without connecting to BigQuery
- Runs on every push and PR
- Fast feedback for syntax errors

### 🏗️ Build & Test
- Creates ephemeral BigQuery dataset (`dbt_ci_pr_<number>` or `dbt_ci_run_<id>`)
- Runs all models and tests
- Automatically cleans up dataset after completion

### 📊 Generate ERD (Main Only)
- Runs only when code is merged to `main`
- Generates Entity Relationship Diagram using `dbterd`
- Commits `docs/erd.md` to the repository
- ERD renders automatically in GitHub (Mermaid format)

### 📚 Generate Docs (PRs Only)
- Generates dbt documentation
- Uploads as artifact (downloadable for 14 days)

## ERD Generation

### How It Works
1. ERD is generated from **relationship tests** in your schema YAML files
2. Only business layer models are included (`dim_*`, `fct_*`, `rpt_*`)
3. Elementary package models are excluded

### Adding New Relationships
To add a relationship to the ERD, add a `relationships` test in your model's YAML:

```yaml
models:
  - name: fct_orders
    columns:
      - name: user_id
        tests:
          - relationships:
              to: ref('dim_users')
              field: user_id
              config:
                severity: warn
```

### Viewing the ERD
- Open `docs/erd.md` in GitHub - it renders as a diagram automatically
- Or copy the Mermaid code to [Mermaid Live Editor](https://mermaid.live/)

## Dataset Naming

| Event | Dataset Name | Example |
|-------|--------------|---------|
| Pull Request | `dbt_ci_pr_<PR_NUMBER>` | `dbt_ci_pr_42` |
| Push to branch | `dbt_ci_run_<RUN_ID>` | `dbt_ci_run_12345678` |

All datasets are automatically deleted after the workflow completes.

## Secrets Required

| Secret Name | Description |
|-------------|-------------|
| `GCP_SERVICE_ACCOUNT_KEY` | GCP service account JSON key with BigQuery permissions |

## Troubleshooting

### Build is slow
- The build job runs all models. Consider implementing Slim CI (only modified models) for faster feedback.

### ERD not showing relationships
- Ensure you have `relationships` tests defined in your schema YAML files
- Only models matching `dim_*`, `fct_*`, `rpt_*` are included

### Merge conflicts in `docs/erd.md`
- This can happen if ERD was generated on multiple branches
- Resolution: `git checkout --theirs docs/erd.md && git add docs/erd.md`
- The ERD will be regenerated correctly on merge to main

## Future Enhancements

- [ ] Slim CI (only build modified models)
- [ ] Production deployment workflow
- [ ] dbt docs hosting (GitHub Pages or similar)
