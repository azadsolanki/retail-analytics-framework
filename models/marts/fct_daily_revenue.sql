{{ config(
    materialized='incremental',
    unique_key='revenue_date',
    incremental_strategy='merge',
    partition_by={
        "field": "revenue_date",
        "data_type": "date"
    },
    tags=['marts', 'finance', 'daily']
) }}

with daily_aggregates as (
    select
        date(order_date) as revenue_date,
        
        -- Revenue metrics
        sum(order_total) as total_revenue,
        sum(net_order_total) as net_revenue,
        sum(returned_amount) as total_returns,
        sum(total_gross_profit) as total_gross_profit,
        avg(order_total) as avg_order_value,
        
        -- Volume metrics
        count(*) as total_orders,
        count(distinct user_id) as unique_customers,
        sum(total_line_items) as total_items_sold,
        sum(unique_products) as total_unique_products_sold,
        
        -- Customer acquisition
        count(case when is_first_order then 1 end) as new_customers,
        count(case when not is_first_order then 1 end) as returning_customers,
        count(case when customer_order_number <= 3 then 1 end) as early_orders,
        
        -- Order characteristics
        count(case when order_size_category = 'Large' then 1 end) as large_orders,
        count(case when order_size_category = 'Medium' then 1 end) as medium_orders,
        count(case when order_size_category = 'Small' then 1 end) as small_orders,
        
        -- Geographic distribution
        count(case when is_domestic_customer then 1 end) as domestic_orders,
        count(case when not is_domestic_customer then 1 end) as international_orders,
        count(distinct customer_state) as states_with_orders,
        count(distinct customer_country) as countries_with_orders,
        
        -- Customer segments
        count(case when customer_segment_at_order = 'VIP' then 1 end) as vip_orders,
        count(case when customer_segment_at_order = 'Loyal' then 1 end) as loyal_orders,
        count(case when customer_segment_at_order = 'Regular' then 1 end) as regular_orders,
        count(case when customer_segment_at_order = 'New' then 1 end) as new_customer_orders,
        
        -- Product categories
        sum(apparel_items) as apparel_items_sold,
        sum(footwear_items) as footwear_items_sold,
        sum(accessories_items) as accessories_items_sold,
        
        -- Price tiers
        sum(luxury_items) as luxury_items_sold,
        sum(premium_items) as premium_items_sold,
        sum(budget_items) as budget_items_sold,
        
        -- Timing patterns
        count(case when is_weekend then 1 end) as weekend_orders,
        count(case when order_time_of_day = 'Evening' then 1 end) as evening_orders,
        count(case when order_time_of_day = 'Morning' then 1 end) as morning_orders,
        
        -- Order status breakdown
        count(case when order_status = 'Complete' then 1 end) as completed_orders,
        count(case when order_status = 'Cancelled' then 1 end) as cancelled_orders,
        count(case when order_has_returns then 1 end) as orders_with_returns,
        
        -- Delivery performance
        avg(delivery_days) as avg_delivery_days,
        count(case when delivery_days <= 3 then 1 end) as fast_deliveries,
        count(case when delivery_days > 7 then 1 end) as slow_deliveries

    from {{ ref('fct_orders') }}
    where order_status in ('Complete', 'Shipped', 'Processing')
    
    {% if is_incremental() %}
        and date(order_date) >= (
            select date_sub(max(revenue_date), interval 3 day)
            from {{ this }}
        )
    {% endif %}
    
    group by 1
)

select
    *,
    
    -- Calculated ratios and percentages
    safe_divide(total_gross_profit, total_revenue) as gross_margin_rate,
    safe_divide(returning_customers, unique_customers) as returning_customer_rate,
    safe_divide(new_customers, unique_customers) as new_customer_rate,
    safe_divide(cancelled_orders, total_orders) as cancellation_rate,
    safe_divide(total_returns, total_revenue) as return_rate,
    safe_divide(international_orders, total_orders) as international_order_rate,
    safe_divide(weekend_orders, total_orders) as weekend_order_rate,
    safe_divide(fast_deliveries, completed_orders) as fast_delivery_rate,
    
    -- Customer acquisition metrics
    safe_divide(total_revenue, new_customers) as revenue_per_new_customer,
    safe_divide(new_customer_orders, new_customers) as orders_per_new_customer,
    
    -- Product mix percentages
    safe_divide(luxury_items_sold, total_items_sold) as luxury_item_mix,
    safe_divide(apparel_items_sold, total_items_sold) as apparel_item_mix,
    
    -- Year-over-year comparisons
    lag(total_revenue, 365) over (order by revenue_date) as revenue_ly,
    safe_divide(
        total_revenue - lag(total_revenue, 365) over (order by revenue_date),
        lag(total_revenue, 365) over (order by revenue_date)
    ) as revenue_yoy_growth,
    
    lag(unique_customers, 365) over (order by revenue_date) as customers_ly,
    safe_divide(
        unique_customers - lag(unique_customers, 365) over (order by revenue_date),
        lag(unique_customers, 365) over (order by revenue_date)
    ) as customer_growth_yoy,
    
    -- Moving averages for trend analysis
    avg(total_revenue) over (
        order by revenue_date 
        rows between 6 preceding and current row
    ) as revenue_7day_avg,
    
    avg(total_revenue) over (
        order by revenue_date 
        rows between 29 preceding and current row
    ) as revenue_30day_avg,
    
    avg(unique_customers) over (
        order by revenue_date 
        rows between 6 preceding and current row
    ) as customers_7day_avg,
    
    -- Performance indicators
    case 
        when total_revenue > avg(total_revenue) over (
            order by revenue_date 
            rows between 29 preceding and current row
        ) * 1.2 then 'High Performance'
        when total_revenue < avg(total_revenue) over (
            order by revenue_date 
            rows between 29 preceding and current row
        ) * 0.8 then 'Low Performance'
        else 'Normal Performance'
    end as daily_performance_status,
    
    current_timestamp() as dbt_updated_at

from daily_aggregates