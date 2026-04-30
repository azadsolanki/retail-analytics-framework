# Retail Analytics Framework

Enterprise-grade dbt project template for retail analytics on BigQuery.

[![dbt](https://img.shields.io/badge/dbt-1.9-FF694B?logo=dbt)](https://www.getdbt.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![CI/CD](https://img.shields.io/badge/CI/CD-GitHub%20Actions-2088FF?logo=github-actions&logoColor=white)](https://github.com/features/actions)
[![Docs](https://img.shields.io/badge/Docs-Live-blue)](https://azadsolanki.github.io/retail-analytics-framework/)

## Overview

A production-ready analytics platform demonstrating enterprise dbt patterns: medallion architecture, semantic layer, model contracts, Slim CI, and automated documentation.

**Data Source**: [TheLook eCommerce](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce) (BigQuery public dataset)

## Architecture

```
Sources → Staging → Intermediate → Marts → Reports
           (Bronze)    (Silver)    (Gold)  (Platinum)
```

| Layer | Purpose | Example |
|-------|---------|---------|
| Staging | Clean, rename, type cast | `stg_orders`, `stg_users` |
| Intermediate | Business logic, enrichment | `int_order_items_enriched` |
| Marts | Dimensional models | `dim_users`, `fct_orders` |
| Reports | Aggregated metrics | `rpt_executive_dashboard` |

## Features

| Feature | Description | Docs |
|---------|-------------|------|
| **Medallion Architecture** | Four-layer data transformation | [Project Structure](#project-structure) |
| **Semantic Layer** | MetricFlow metrics definitions | [models/semantic/](models/semantic/) |
| **Model Contracts** | Schema enforcement for marts | [docs/MODEL_CONTRACTS.md](docs/MODEL_CONTRACTS.md) |
| **Slim CI** | Only build modified models | [docs/CI_CD_README.md](docs/CI_CD_README.md) |
| **Auto ERD** | Generated from relationship tests | [docs/erd.md](docs/erd.md) |
| **GitHub Pages Docs** | Live dbt documentation | [View Docs](https://azadsolanki.github.io/retail-analytics-framework/) |

## Quick Start

```bash
# Clone
git clone https://github.com/azadsolanki/retail-analytics-framework.git
cd retail-analytics-framework

# Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configure ~/.dbt/profiles.yml with your GCP project

# Build
dbt deps
dbt build
```

## Project Structure

```
├── models/
│   ├── staging/          # Bronze: source cleaning
│   ├── intermediate/     # Silver: business logic
│   ├── marts/            # Gold: dimensional models
│   ├── reports/          # Platinum: aggregations
│   └── semantic/         # MetricFlow definitions
├── tests/                # Custom data tests
├── macros/               # Reusable SQL
├── seeds/                # Reference data
├── snapshots/            # SCD Type 2
├── scripts/              # Dev utilities
│   ├── dev-setup.sh      # Download prod manifest
│   ├── slim-build.sh     # Build modified only
│   └── list-modified.sh  # Show changes
└── docs/                 # Documentation
    ├── CI_CD_README.md
    ├── MODEL_CONTRACTS.md
    └── erd.md
```

## CI/CD Pipeline

| Trigger | Jobs |
|---------|------|
| Push to `feature/**` | Compile → Build |
| Pull Request | Compile → Slim Build → Docs |
| Merge to `main` | Compile → Full Build → ERD → Deploy Docs |

Manifests stored in GCS for Slim CI. Datasets auto-cleanup after runs.

## Key Models

| Model | Type | Description |
|-------|------|-------------|
| `dim_users` | Dimension | Customer demographics, RFM scores, segments |
| `dim_products` | Dimension | Product catalog, performance tiers |
| `fct_orders` | Fact | Order transactions, customer journey |
| `fct_daily_revenue` | Fact | Daily revenue aggregations |

## Semantic Layer

Metrics defined in MetricFlow for consistent analytics:

```bash
mf query --metrics revenue,aov --group-by order__order_date
```

| Metric | Description |
|--------|-------------|
| `revenue` | Total order revenue |
| `gross_margin` | Profit as % of revenue |
| `aov` | Average order value |
| `customer_ltv` | Average lifetime value |

## Development

### Local Slim CI

```bash
./scripts/dev-setup.sh    # Download prod manifest
./scripts/slim-build.sh   # Build only changed models
```

### Testing

```bash
dbt test                           # All tests
dbt test --select dim_users        # Specific model
```

## Documentation

| Doc | Description |
|-----|-------------|
| [CI/CD Guide](docs/CI_CD_README.md) | Pipeline setup, Slim CI |
| [Model Contracts](docs/MODEL_CONTRACTS.md) | Schema enforcement |
| [Semantic Layer](models/semantic/README.md) | MetricFlow usage |
| [ERD](docs/erd.md) | Auto-generated diagram |
| [Live Docs](https://azadsolanki.github.io/retail-analytics-framework/) | Interactive dbt docs |

## Roadmap

- [x] Medallion architecture
- [x] CI/CD with GitHub Actions
- [x] Slim CI with GCS manifests
- [x] Semantic Layer (MetricFlow)
- [x] Model Contracts
- [x] Auto ERD generation
- [ ] Airflow DAGs
- [ ] Multi-environment (dev/staging/prod)
- [ ] dbt Mesh for multi-project

## License

MIT
