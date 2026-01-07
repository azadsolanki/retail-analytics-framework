{ config(
    materialized='table',
    tags=['marts', 'core', 'dimension']
) }}

select
    p.product_id,
    p.product_name,
    p.category,
    p.category_group,
    p.brand,
    p.brand_tier,
    p.department,
    p.sku,
    p.cost as product_cost,
    p.retail_price,
    p.margin_amount,
    p.margin_percentage,
    p.margin_tier,
    p.price_category,
    p.distribution_center_id,
    
    -- Sales performance metrics
    coalesce(pm.total_units_sold, 0) as total_units_sold,
    coalesce(pm.total_orders, 0) as total_orders,
    coalesce(pm.unique_customers, 0) as unique_customers,
    coalesce(pm.total_revenue, 0) as total_revenue,
    coalesce(pm.total_gross_profit, 0) as total_gross_profit,
    coalesce(pm.avg_selling_price, p.retail_price) as avg_selling_price,
    coalesce(pm.avg_gross_margin_pct, p.margin_percentage) as avg_gross_margin_pct,
    
    -- Return metrics
    coalesce(pm.units_returned, 0) as units_returned,
    coalesce(pm.total_returned_revenue, 0) as total_returned_revenue,
    coalesce(pm.return_rate, 0) as return_rate,
    
    -- Performance indicators
    pm.first_sale_date,
    pm.last_sale_date,
    coalesce(pm.active_sales_days, 0) as active_sales_days,
    coalesce(pm.revenue_90d, 0) as revenue_90d,
    coalesce(pm.units_sold_90d, 0) as units_sold_90d,
    coalesce(pm.recent_performance_status, 'No Sales') as recent_performance_status,
    
    -- Customer and geographic metrics
    coalesce(pm.revenue_per_customer, 0) as revenue_per_customer,
    coalesce(pm.avg_units_per_order, 0) as avg_units_per_order,
    coalesce(pm.geographic_reach_score, 0) as geographic_reach_score,
    coalesce(pm.customer_diversity_pct, 0) as customer_diversity_pct,
    
    -- Seasonality
    coalesce(pm.q4_seasonality_index, 0) as q4_seasonality_index,
    
    -- Performance categorization
    case 
        when pm.total_revenue >= 50000 then 'Star'
        when pm.total_revenue >= 20000 then 'High Performer'
        when pm.total_revenue >= 5000 then 'Good Performer'
        when pm.total_revenue >= 1000 then 'Average Performer'
        when pm.total_revenue > 0 then 'Slow Mover'
        else 'No Sales'
    end as performance_tier,
    
    -- Inventory status
    case 
        when pm.revenue_90d > 1000 then 'Hot'
        when pm.revenue_90d > 100 then 'Moving'
        when pm.revenue_90d > 0 then 'Slow'
        else 'Dead Stock'
    end as inventory_status,
    
    -- Pricing analysis
    case 
        when pm.avg_selling_price > p.retail_price * 1.1 then 'Premium Priced'
        when pm.avg_selling_price < p.retail_price * 0.9 then 'Discounted'
        else 'Standard Priced'
    end as pricing_position,
    
    current_timestamp() as dbt_updated_at

from {{ ref('stg_products') }} as p
left join {{ ref('int_product_metrics') }} as pm
    on p.product_id = pm.product_id
where not p.has_invalid_cost
  and not p.has_negative_margin
  and not p.has_missing_name