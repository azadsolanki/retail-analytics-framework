{{ config(
    MATERIALIZED='table',
    tags=['marts', 'core', 'dimension']
)}}

select
    u.user_id,
    u.first_name,
    u.last_name,
    u.full_name,
    u.email,
    u.age,
    u.age_group,
    u.generation,
    u.gender,
    u.state,
    u.country,
    u.region,
    u.is_domestic,
    u.latitude,
    u.longitude,
    u.traffic_source,
    u.acquisition_channel,
    u.registration_date,
    
    -- Order history metrics
    coalesce(oh.total_orders, 0) as total_orders,
    coalesce(oh.completed_orders, 0) as completed_orders,
    coalesce(oh.cancelled_orders, 0) as cancelled_orders,
    coalesce(oh.lifetime_value, 0) as lifetime_value,
    coalesce(oh.net_lifetime_value, 0) as net_lifetime_value,
    coalesce(oh.lifetime_gross_profit, 0) as lifetime_gross_profit,
    coalesce(oh.avg_order_value, 0) as avg_order_value,
    coalesce(oh.avg_item_value, 0) as avg_item_value,
    
    -- Timing metrics
    oh.first_order_date,
    oh.last_order_date,
    coalesce(oh.days_since_last_order, 999999) as days_since_last_order,
    coalesce(oh.customer_tenure_days, 0) as customer_tenure_days,
    coalesce(oh.orders_per_month, 0) as orders_per_month,
    coalesce(oh.orders_per_year, 0) as orders_per_year,
    
    -- Product preferences
    coalesce(oh.unique_products_purchased, 0) as unique_products_purchased,
    coalesce(oh.unique_categories_purchased, 0) as unique_categories_purchased,
    coalesce(oh.unique_brands_purchased, 0) as unique_brands_purchased,
    oh.preferred_category,
    oh.preferred_brand,
    coalesce(oh.product_diversity_score, 0) as product_diversity_score,
    
    -- Behavioral patterns
    coalesce(oh.return_rate, 0) as return_rate,
    coalesce(oh.weekend_order_pct, 0) as weekend_order_pct,
    coalesce(oh.premium_propensity_pct, 0) as premium_propensity_pct,
    coalesce(oh.avg_delivery_days, 0) as avg_delivery_days,
    coalesce(oh.fast_delivery_pct, 0) as fast_delivery_pct,
    
    -- Customer segmentation
    case 
        when oh.lifetime_value >= {{ var('vip_threshold') }} then 'VIP'
        when oh.completed_orders >= 5 then 'Loyal'
        when oh.completed_orders >= 2 then 'Regular'
        when oh.completed_orders = 1 then 'New'
        else 'Prospect'
    end as value_segment,
    
    case 
        when oh.days_since_last_order <= {{ var('active_days') }} then 'Active'
        when oh.days_since_last_order <= 180 then 'At Risk'
        when oh.days_since_last_order <= 365 then 'Dormant'
        when oh.days_since_last_order < 999999 then 'Churned'
        else 'Prospect'
    end as activity_segment,
    
    case
        when oh.orders_per_month >= 2 then 'High Frequency'
        when oh.orders_per_month >= 0.5 then 'Medium Frequency'
        when oh.orders_per_month > 0 then 'Low Frequency'
        else 'One-time'
    end as frequency_segment,
    
    -- RFM Score Components
    case 
        when oh.days_since_last_order <= 30 then 5
        when oh.days_since_last_order <= 90 then 4
        when oh.days_since_last_order <= 180 then 3
        when oh.days_since_last_order <= 365 then 2
        else 1
    end as recency_score,
    
    case 
        when oh.completed_orders >= 10 then 5
        when oh.completed_orders >= 5 then 4
        when oh.completed_orders >= 3 then 3
        when oh.completed_orders >= 2 then 2
        else 1
    end as frequency_score,
    
    case 
        when oh.lifetime_value >= 1000 then 5
        when oh.lifetime_value >= 500 then 4
        when oh.lifetime_value >= 200 then 3
        when oh.lifetime_value >= 100 then 2
        else 1
    end as monetary_score,
    
    current_timestamp() as dbt_updated_at

from {{ ref('stg_users') }} as u
left join {{ ref('int_user_order_history') }} as oh
    on u.user_id = oh.user_id
where not u.has_invalid_email
  and not u.has_invalid_age
