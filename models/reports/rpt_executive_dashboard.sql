{{ config(
    materialized='table',
    tags=['reports', 'executive']
) }}

with monthly_metrics as (
    select
        date_trunc(revenue_date, month) as metric_month,
        sum(total_revenue) as monthly_revenue,
        sum(net_revenue) as monthly_net_revenue,
        avg(avg_order_value) as monthly_avg_order_value,
        sum(unique_customers) as monthly_active_customers,
        sum(new_customers) as monthly_new_customers,
        sum(total_orders) as monthly_orders,
        avg(gross_margin_rate) as monthly_gross_margin_rate,
        avg(return_rate) as monthly_return_rate,
        avg(cancellation_rate) as monthly_cancellation_rate,
        avg(fast_delivery_rate) as monthly_fast_delivery_rate
    from {{ ref('fct_daily_revenue') }}
    group by 1
),

customer_metrics as (
    select
        date_trunc(current_date(), month) as metric_month,
        count(*) as total_customers,
        count(case when value_segment = 'VIP' then 1 end) as vip_customers,
        count(case when activity_segment = 'Active' then 1 end) as active_customers,
        count(case when activity_segment = 'At Risk' then 1 end) as at_risk_customers,
        count(case when activity_segment = 'Churned' then 1 end) as churned_customers,
        avg(lifetime_value) as avg_customer_ltv,
        avg(case when total_orders > 0 then total_orders end) as avg_orders_per_customer
    from {{ ref('dim_users') }}
),

product_metrics as (
    select
        date_trunc(current_date(), month) as metric_month,
        count(*) as total_products,
        count(case when inventory_status = 'Hot' then 1 end) as hot_products,
        count(case when inventory_status = 'Dead Stock' then 1 end) as dead_stock_products,
        count(case when performance_tier = 'Star' then 1 end) as star_products,
        avg(case when total_revenue > 0 then return_rate end) as avg_product_return_rate
    from {{ ref('dim_products') }}
)

select
    mm.metric_month,
    
    -- Revenue metrics
    mm.monthly_revenue,
    mm.monthly_net_revenue,
    mm.monthly_avg_order_value,
    mm.monthly_gross_margin_rate,
    
    -- Customer metrics
    mm.monthly_active_customers,
    mm.monthly_new_customers,
    cm.total_customers,
    cm.vip_customers,
    cm.active_customers,
    cm.at_risk_customers,
    cm.avg_customer_ltv,
    
    -- Order metrics
    mm.monthly_orders,
    mm.monthly_return_rate,
    mm.monthly_cancellation_rate,
    mm.monthly_fast_delivery_rate,
    
    -- Product metrics
    pm.total_products,
    pm.hot_products,
    pm.dead_stock_products,
    pm.star_products,
    pm.avg_product_return_rate,
    
    -- Growth calculations
    lag(mm.monthly_revenue) over (order by mm.metric_month) as prev_month_revenue,
    round((mm.monthly_revenue - lag(mm.monthly_revenue) over (order by mm.metric_month)) 
          / lag(mm.monthly_revenue) over (order by mm.metric_month) * 100, 2) as revenue_growth_mom,
    
    lag(mm.monthly_revenue, 12) over (order by mm.metric_month) as same_month_ly_revenue,
    round((mm.monthly_revenue - lag(mm.monthly_revenue, 12) over (order by mm.metric_month)) 
          / lag(mm.monthly_revenue, 12) over (order by mm.metric_month) * 100, 2) as revenue_growth_yoy,
    
    -- Customer acquisition efficiency
    round(mm.monthly_revenue / mm.monthly_new_customers, 2) as revenue_per_new_customer,
    round(mm.monthly_new_customers * 100.0 / cm.total_customers, 2) as new_customer_rate,
    
    -- Operational efficiency
    round(cm.active_customers * 100.0 / cm.total_customers, 2) as customer_activity_rate,
    round(pm.hot_products * 100.0 / pm.total_products, 2) as hot_product_rate,
    round(pm.dead_stock_products * 100.0 / pm.total_products, 2) as dead_stock_rate,
    
    current_timestamp() as dbt_updated_at

from monthly_metrics mm
left join customer_metrics cm
    on mm.metric_month = cm.metric_month
left join product_metrics pm
    on mm.metric_month = pm.metric_month
order by mm.metric_month desc