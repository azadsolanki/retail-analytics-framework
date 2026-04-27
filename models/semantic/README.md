# Semantic Layer

This directory contains MetricFlow semantic models and metrics definitions for the Retail Analytics Framework.

## What is the Semantic Layer?

The Semantic Layer provides a single source of truth for business metrics. Instead of defining metrics in multiple BI tools, metrics are defined once here and consumed everywhere.

## Structure

```
models/semantic/
├── sem_orders.yml          # Orders fact semantic model
├── sem_customers.yml       # Customers dimension semantic model
├── sem_products.yml        # Products dimension semantic model
├── sem_order_items.yml     # Order items bridge semantic model
├── metrics.yml             # Business metric definitions
├── metricflow_time_spine.sql   # Required time dimension
├── _time_spine.yml         # Time spine configuration
└── README.md               # This file
```

## Semantic Models

| Model | Description | Primary Entity |
|-------|-------------|----------------|
| `orders` | Order transactions | `order` (order_id) |
| `customers` | Customer dimension | `customer` (user_id) |
| `products` | Product dimension | `product` (product_id) |
| `order_items` | Order line items | `order_item` (order_item_id) |

## Key Metrics

### Revenue Metrics
| Metric | Description |
|--------|-------------|
| `revenue` | Total revenue from completed orders |
| `gross_profit` | Total gross profit |
| `gross_margin` | Gross profit as % of revenue |
| `aov` | Average order value |

### Customer Metrics
| Metric | Description |
|--------|-------------|
| `customers` | Total customer count |
| `new_customers` | First-time customers |
| `customer_ltv` | Average lifetime value |
| `churn_rate` | % of churned customers |

### Product Metrics
| Metric | Description |
|--------|-------------|
| `products_sold` | Total items sold |
| `product_revenue` | Revenue from product sales |
| `return_rate` | % of items returned |

## Usage

### Local Development (dbt Core)

```bash
# Validate semantic models
mf validate-configs

# List available metrics
mf list metrics

# Query a metric
mf query --metrics revenue --group-by order__order_date

# Query with dimensions
mf query --metrics revenue,orders --group-by order__order_status
```

### dbt Cloud

With dbt Cloud, metrics are available via:
- Semantic Layer API (GraphQL, JDBC)
- Direct integrations (Tableau, Hex, Mode, Google Sheets)
- AI agents via MCP

## Adding New Metrics

1. If needed, add a new measure to the appropriate semantic model
2. Define the metric in `metrics.yml`
3. Run `mf validate-configs` to verify
4. Test with `mf query`

Example metric:

```yaml
metrics:
  - name: my_new_metric
    description: What this metric measures
    type: simple  # or derived, ratio, cumulative
    label: Display Name
    type_params:
      measure: measure_name
```

## Joins

MetricFlow automatically handles joins based on entity relationships:

```
customers (customer) <-- orders (customer, order) <-- order_items (order, product) --> products (product)
```

This allows queries like:
```bash
mf query --metrics revenue --group-by customer__value_segment
```

## Requirements

- dbt 1.6+
- MetricFlow (included with dbt-core)
- For API access: dbt Cloud Team or Enterprise plan
