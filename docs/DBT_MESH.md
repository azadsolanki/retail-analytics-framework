# dbt Mesh Architecture

This project is **mesh-ready** - structured for multi-project dbt deployment.

## Mesh Concepts Implemented

### 1. Access Levels

| Access | Description | Usage |
|--------|-------------|-------|
| `public` | Exposed to other projects | Mart models (dim_*, fct_*) |
| `protected` | Internal to this project | Staging, intermediate models |
| `private` | Internal to group only | Sensitive or experimental models |

### 2. Groups (Team Ownership)

| Group | Owner | Models |
|-------|-------|--------|
| `data_engineering` | Data Engineering Team | Staging, intermediate |
| `analytics_engineering` | Analytics Engineering Team | Marts, reports |
| `data_science` | Data Science Team | ML features, predictions |

### 3. Model Versions

Public models support versioning for breaking changes:

```yaml
models:
  - name: dim_users
    latest_version: 1
    versions:
      - v: 1
        description: "Initial version"
      - v: 2
        description: "Added new columns"
        columns:
          - include: all
          - name: new_column
```

Reference specific versions:
```sql
-- Latest version
select * from {{ ref('dim_users') }}

-- Specific version
select * from {{ ref('dim_users', v=1) }}
```

### 4. Contracts

Public models enforce contracts:

```yaml
models:
  - name: dim_users
    access: public
    config:
      contract:
        enforced: true
    columns:
      - name: user_id
        data_type: string
        constraints:
          - type: not_null
          - type: primary_key
```

## Cross-Project References

When splitting into multiple projects, use:

```sql
-- Reference model from another project
select * from {{ ref('staging_project', 'stg_orders') }}
```

## Project Structure for Full Mesh

```
retail-platform/
├── staging/                    # Project: raw data ingestion
│   ├── dbt_project.yml
│   └── models/
│       └── stg_*.sql          # access: public (for core project)
│
├── core/                       # Project: business models
│   ├── dbt_project.yml
│   ├── dependencies.yml        # depends on: staging
│   └── models/
│       ├── intermediate/       # access: protected
│       └── marts/              # access: public (for analytics)
│
└── analytics/                  # Project: reporting
    ├── dbt_project.yml
    ├── dependencies.yml        # depends on: core
    └── models/
        └── reports/            # access: public
```

## Dependencies File

For multi-project mesh, create `dependencies.yml`:

```yaml
# analytics/dependencies.yml
projects:
  - name: core

packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

## Benefits of Mesh Architecture

| Benefit | Description |
|---------|-------------|
| **Team autonomy** | Teams own their projects independently |
| **Clear contracts** | Public interfaces are versioned and enforced |
| **Faster CI** | Only rebuild affected projects |
| **Better governance** | Access controls prevent misuse |
| **Scalability** | Add projects without affecting others |

## Migration Path

1. **Current state**: Single project, mesh-ready (groups, access, versions)
2. **Next step**: Extract staging to separate project
3. **Full mesh**: Three projects (staging → core → analytics)

## References

- [dbt Mesh Documentation](https://docs.getdbt.com/best-practices/how-we-mesh/mesh-1-intro)
- [Model Access](https://docs.getdbt.com/docs/collaborate/govern/model-access)
- [Model Versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions)
- [Model Contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)
