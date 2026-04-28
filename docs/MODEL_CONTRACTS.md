# Model Contracts

Model contracts enforce schema guarantees for your dbt models. When a contract is enforced, dbt validates that the model produces exactly the columns specified with the correct data types.

## Why Use Contracts?

- **Breaking change prevention**: CI fails if someone removes or renames a column
- **Type safety**: Ensures data types match expectations
- **Documentation**: Contracts serve as schema documentation
- **Team coordination**: Downstream teams can depend on stable interfaces

## Contracted Models

| Model | Description |
|-------|-------------|
| `dim_users` | Customer dimension |
| `dim_products` | Product dimension |
| `fct_orders` | Order transactions |

## How It Works

When `contract.enforced: true` is set:

1. dbt compares model output against the contract
2. Missing columns → build fails
3. Wrong data types → build fails
4. Extra columns → allowed (non-breaking)

## Example Contract

```yaml
models:
  - name: dim_users
    config:
      contract:
        enforced: true
    columns:
      - name: user_id
        data_type: int64
        constraints:
          - type: not_null
          - type: primary_key
      - name: email
        data_type: string
```

## Adding New Columns

1. Add the column to your model SQL
2. Add the column to `_contracts.yml` with data type
3. Run `dbt build` to validate

## Changing Existing Columns

Breaking changes require coordination:

1. Notify downstream consumers
2. Update `_contracts.yml`
3. Update model SQL
4. Deploy together

## Constraints

Available constraints:

| Constraint | Description |
|------------|-------------|
| `not_null` | Column cannot be null |
| `primary_key` | Unique identifier (implies not_null) |
| `unique` | All values must be unique |
| `foreign_key` | References another table |

Note: BigQuery enforces constraints at query time, not insert time.

## Testing Contracts Locally

```bash
# Build with contract validation
dbt build --select dim_users

# If contract fails, you'll see:
# Compilation Error: This model has an enforced contract...
```
