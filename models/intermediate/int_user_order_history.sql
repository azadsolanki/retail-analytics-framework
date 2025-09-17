{{ config(
    MATERIALIZED='ephemeral',
    tags=['intermediate']
) }}

with order_aggregates as (
    select
        user_id,
        
        -- Order counts by status
        count(*) as total_orders,
        count(case when order_status = 'Complete' then 1 end) as completed_orders,
        count(case when order_status = 'Cancelled' then 1 end) as cancelled_orders,
        count(case when order_status = 'Returned' then 1 end) as returned_orders,
        count(case when is_returned then 1 end) as orders_with_returns,
        
        -- Financial metrics
        sum(line_total) as lifetime_value,
        sum(case when not is_returned then line_total else 0 end) as net_lifetime_value,
        sum(item_gross_profit) as lifetime_gross_profit,
        sum(returned_value) as total_returned_value,
        avg(line_total) as avg_item_value,
        
        -- Order characteristics
        avg(delivery_days) as avg_delivery_days,
        sum(case when delivery_days <= 3 then 1 else 0 end) as fast_deliveries,
        
        -- Product preferences
        count(distinct product_id) as unique_products_purchased,
        count(distinct category) as unique_categories_purchased,
        count(distinct brand) as unique_brands_purchased,
        count(distinct department) as unique_departments_purchased,
        
        -- Timing patterns
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(distinct date(order_date)) as unique_order_days,
        
        -- Channel and behavior
        count(case when is_weekend then 1 end) as weekend_orders,
        count(case when order_time_of_day = 'Evening' then 1 end) as evening_orders,
        
        -- Product tier preferences
        count(case when price_category = 'Luxury' then 1 end) as luxury_items,
        count(case when price_category = 'Premium' then 1 end) as premium_items,
        count(case when price_category = 'Budget' then 1 end) as budget_items,
        
        -- Brand preferences
        count(case when brand_tier = 'Premium' then 1 end) as premium_brand_items,
        
 
        -- Top category (most frequent)
        array_agg(category_group order by category_group limit 1)[offset(0)] as preferred_category,

        -- Top brand (most frequent)  
        array_agg(brand order by brand limit 1)[offset(0)] as preferred_brand


    from {{ ref('int_order_items_enriched') }}
    where order_complete
    group by user_id
)

select
    *,
    
    -- Derived metrics
    date_diff(current_date(), date(last_order_date), day) as days_since_last_order,
    date_diff(last_order_date, first_order_date, day) as customer_tenure_days,
  
   -- Frequency calculations
    case when date_diff(last_order_date, first_order_date, day) > 0 then
        round(total_orders * 365.0 / date_diff(last_order_date, first_order_date, day), 2)
        else 0 end as orders_per_year,
        
    case when date_diff(last_order_date, first_order_date, day) > 30 then
        round(total_orders * 30.0 / date_diff(last_order_date, first_order_date, day), 2)
        else total_orders end as orders_per_month,
    
    -- Value calculations
    case when completed_orders > 0 then
        round(lifetime_value / completed_orders, 2)
        else 0 end as avg_order_value,
        
    case when lifetime_value > 0 then
        round(lifetime_gross_profit / lifetime_value * 100, 2)
        else 0 end as avg_margin_pct,
    
    -- Behavioral patterns
    case when total_orders > 0 then
        round(orders_with_returns * 100.0 / total_orders, 2)
        else 0 end as return_rate,
        
    case when total_orders > 0 then
        round(weekend_orders * 100.0 / total_orders, 2)
        else 0 end as weekend_order_pct,
        
    case when completed_orders > 0 then
        round(fast_deliveries * 100.0 / completed_orders, 2)
        else 0 end as fast_delivery_pct,
    
    -- Product diversity
    case when total_orders > 0 then
        round(unique_products_purchased * 1.0 / total_orders, 2)
        else 0 end as product_diversity_score,
    
    -- Premium propensity
    case when total_orders > 0 then
        round((luxury_items + premium_items) * 100.0 / total_orders, 2)
        else 0 end as premium_propensity_pct

from order_aggregates