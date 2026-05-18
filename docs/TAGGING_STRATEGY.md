# Tagging Strategy

Consistent tags for organizing, running, and monitoring dbt models.

## Tag Categories

| Category | Tags | Purpose |
|----------|------|---------|
| **Frequency** | `hourly`, `daily`, `weekly`, `monthly` | Schedule-based runs |
| **Priority** | `tier1`, `tier2`, `tier3` | SLA and testing priority |
| **Domain** | `finance`, `marketing`, `product`, `operations` | Business area |
| **Layer** | `staging`, `intermediate`, `marts`, `reports` | Data layer |

## Tag Definitions

### Frequency Tags

| Tag | Description | SLA |
|-----|-------------|-----|
| `hourly` | Near real-time models | < 1 hour freshness |
| `daily` | Standard refresh | < 24 hour freshness |
| `weekly` | Weekly aggregations | Monday refresh |
| `monthly` | Month-end models | 1st of month |

### Priority Tags

| Tag | Description | Testing | Alerting |
|-----|-------------|---------|----------|
| `tier1` | Business critical | Full tests in CI | Immediate alert |
| `tier2` | Important | Basic tests in CI | Daily digest |
| `tier3` | Nice to have | Tests on merge only | Weekly review |

### Domain Tags

| Tag | Owner | Models |
|-----|-------|--------|
| `finance` | Finance team | Revenue, costs, margins |
| `marketing` | Marketing team | Campaigns, attribution |
| `product` | Product team | Usage, features |
| `operations` | Ops team | Inventory, fulfillment |

## Usage

### Selective Runs

```bash
# Run only daily models
dbt run --select tag:daily

# Run finance domain
dbt run --select tag:finance

# Run tier1 models only
dbt run --select tag:tier1

# Combine tags
dbt run --select tag:daily,tag:tier1
```

### CI/CD Integration

```yaml
# In GitHub Actions
- name: Test critical models
  run: dbt test --select tag:tier1

- name: Build daily models
  run: dbt run --select tag:daily
```

### Airflow DAGs

```python
# Separate DAGs by frequency
daily_models = BashOperator(
    task_id="daily_models",
    bash_command="dbt run --select tag:daily",
)

hourly_models = BashOperator(
    task_id="hourly_models",
    bash_command="dbt run --select tag:hourly",
)
```

## Model Assignments

### Staging (protected, tier2)

| Model | Frequency | Domain |
|-------|-----------|--------|
| `stg_orders` | daily | finance |
| `stg_users` | daily | marketing |
| `stg_products` | daily | product |
| `stg_order_items` | daily | finance |
| `stg_events` | hourly | product |

### Marts (public, tier1)

| Model | Frequency | Domain |
|-------|-----------|--------|
| `dim_users` | daily | marketing |
| `dim_products` | daily | product |
| `fct_orders` | daily | finance |
| `fct_daily_revenue` | daily | finance |

### Reports (public, tier2)

| Model | Frequency | Domain |
|-------|-----------|--------|
| `rpt_executive_dashboard` | daily | finance |
| `rpt_customer_cohorts` | weekly | marketing |
| `rpt_product_performance` | daily | product |

## Best Practices

1. **Every model gets at least**: frequency + priority + domain
2. **Layer tags are automatic** via `dbt_project.yml`
3. **Tier1 = contracts enforced**
4. **Use tags in alerts**: "Tier1 model failed" vs generic alert
