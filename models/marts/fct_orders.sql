{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns',
    partition_by={
        "field": "order_date",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=["user_id", "order_status"],
    tags=['marts', 'core', 'incremental']
) }}

with order_calculations as (
    select
        o.order_id,
        o.user_id,
        o.order_date,
        o.order_status,
        o.customer_gender,
        o.order_time_of_day,
        o.order_day_name,
        o.order_hour,
        o.order_month,
        o.order_quarter,
        o.order_year,
        o.is_weekend,
        o.item_count,
        o.is_returned as order_has_returns,
        o.is_delivered,
        o.is_complete,
       max(o.delivery_days) as delivery_days,
        
        -- Financial aggregations from order items
        sum(oi.line_total) as order_total,
        sum(oi.item_gross_profit) as total_gross_profit,
        avg(oi.item_gross_margin_pct) as avg_gross_margin_pct,
        sum(case when oi.is_returned then oi.line_total else 0 end) as returned_amount,
        sum(case when not oi.is_returned then oi.line_total else 0 end) as net_order_total,
        
        -- Product mix analysis
        count(distinct oi.product_id) as unique_products,
        count(distinct oi.category) as unique_categories,
        count(distinct oi.brand) as unique_brands,
        count(*) as total_line_items,
        
        -- Price analysis
        avg(oi.unit_price) as avg_item_price,
        max(oi.unit_price) as max_item_price,
        min(oi.unit_price) as min_item_price,
        stddev(oi.unit_price) as price_stddev,
        
        -- Category mix
        count(case when oi.category_group = 'Apparel' then 1 end) as apparel_items,
        count(case when oi.category_group = 'Footwear' then 1 end) as footwear_items,
        count(case when oi.category_group = 'Accessories' then 1 end) as accessories_items,
        
        -- Price tier mix
        count(case when oi.price_category = 'Luxury' then 1 end) as luxury_items,
        count(case when oi.price_category = 'Premium' then 1 end) as premium_items,
        count(case when oi.price_category = 'Budget' then 1 end) as budget_items,
        
        -- Customer demographics from enriched data
        max(oi.age) as customer_age,
        max(oi.age_group) as customer_age_group,
        max(oi.generation) as customer_generation,
        max(oi.state) as customer_state,
        max(oi.country) as customer_country,
        max(oi.region) as customer_region,
        max(oi.is_domestic) as is_domestic_customer,
        max(oi.acquisition_channel) as customer_acquisition_channel

    from {{ ref('stg_orders') }} as o
    inner join {{ ref('int_order_items_enriched') }} as oi
        on o.order_id = oi.order_id
    where not o.has_missing_user
      and not o.has_invalid_item_count
      and not o.is_future_order
    
    {% if is_incremental() %}
        -- Incremental processing with lookback for late-arriving data
        and o.order_date >= (
            select date_sub(max(order_date), interval 3 day)
            from {{ this }}
        )
    {% endif %}
    
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
),

customer_sequence as (
    select
        *,
        
        -- Customer order sequence analysis
        row_number() over (
            partition by user_id 
            order by order_date
        ) as customer_order_number,
        
        -- Running customer metrics
        sum(order_total) over (
            partition by user_id 
            order by order_date 
            rows unbounded preceding
        ) as customer_running_total,
        
        count(*) over (
            partition by user_id 
            order by order_date 
            rows unbounded preceding
        ) as customer_running_orders,
        
        -- Inter-order timing
        lag(order_date) over (
            partition by user_id 
            order by order_date
        ) as previous_order_date,
        
        date_diff(
            order_date,
            lag(order_date) over (partition by user_id order by order_date),
            day
        ) as days_since_previous_order,
        
        -- Order value progression
        lag(order_total) over (
            partition by user_id 
            order by order_date
        ) as previous_order_value,
        
        -- Customer segment at time of order
        case 
            when sum(order_total) over (
                partition by user_id 
                order by order_date 
                rows unbounded preceding
            ) >= {{ var('vip_threshold') }} then 'VIP'
            when count(*) over (
                partition by user_id 
                order by order_date 
                rows unbounded preceding
            ) >= 5 then 'Loyal'
            when count(*) over (
                partition by user_id 
                order by order_date 
                rows unbounded preceding
            ) >= 2 then 'Regular'
            else 'New'
        end as customer_segment_at_order

    from order_calculations
)

select
    *,
    
    -- Order characteristics
    case when customer_order_number = 1 then true else false end as is_first_order,
    case when customer_order_number <= 3 then true else false end as is_early_order,
    
    -- Order size categorization
    case 
        when order_total >= {{ var('high_value_order') }} then 'Large'
        when order_total >= 100 then 'Medium'
        when order_total >= 50 then 'Small'
        else 'Micro'
    end as order_size_category,
    
    -- Product diversity metrics
    safe_divide(unique_products, total_line_items) as product_diversity_ratio,
    safe_divide(unique_categories, total_line_items) as category_diversity_ratio,
    
    -- Return analysis
    safe_divide(returned_amount, order_total) as return_rate_by_value,
    case when returned_amount > 0 then true else false end as has_returns,
    
    -- Value progression
    case 
        when previous_order_value is null then 'First Order'
        when order_total > previous_order_value * 1.2 then 'Increasing Value'
        when order_total < previous_order_value * 0.8 then 'Decreasing Value'
        else 'Stable Value'
    end as value_trend,
    
    -- Repurchase timing
    case 
        when days_since_previous_order is null then 'First Order'
        when days_since_previous_order <= 30 then 'Quick Repeat'
        when days_since_previous_order <= 90 then 'Regular Repeat'
        when days_since_previous_order <= 180 then 'Slow Repeat'
        else 'Long Gap'
    end as repurchase_timing,
    
    -- Seasonal indicators
    case 
        when order_month in (11, 12) then 'Holiday Season'
        when order_month in (6, 7, 8) then 'Summer'
        when order_month in (3, 4, 5) then 'Spring'
        else 'Regular Season'
    end as seasonal_period,
    
    current_timestamp() as dbt_updated_at

from customer_sequence