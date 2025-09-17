{{ config(
    materialized='ephemeral',
    tags=['intermediate']
) }}

select
    oi.order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    oi.inventory_item_id,
    oi.quantity,
    oi.sale_price as unit_price,
    oi.line_total,
    oi.discount_amount,
    oi.is_returned,
    oi.is_shipped,
    oi.is_delivered,
    oi.order_item_date,
    
    -- Product enrichment
    p.product_name,
    p.category,
    p.category_group,
    p.brand,
    p.brand_tier,
    p.department,
    p.cost as product_cost,
    p.retail_price,
    p.margin_amount as product_margin,
    p.margin_percentage as product_margin_pct,
    p.margin_tier,
    p.price_category,
    
    -- Profitability calculations
    oi.sale_price - p.cost as item_gross_profit,
    case when oi.sale_price > 0 then
        round((oi.sale_price - p.cost) / oi.sale_price * 100, 2)
        else 0 end as item_gross_margin_pct,
    
    -- Price analysis
    oi.sale_price - p.retail_price as price_variance,
    case when p.retail_price > 0 then
        round((oi.sale_price - p.retail_price) / p.retail_price * 100, 2)
        else 0 end as price_variance_pct,
        
    case
        when oi.sale_price > p.retail_price * 1.05 then 'Premium Pricing'
        when oi.sale_price < p.retail_price * 0.95 then 'Discount Pricing'
        else 'Standard Pricing'
    end as pricing_strategy,
    
    -- Order context
    o.order_date,
    o.order_status,
    o.order_time_of_day,
    o.order_day_name,
    o.is_weekend,
    o.customer_gender,
    o.delivery_days,
    o.is_complete as order_complete,
    
    -- User context
    u.age,
    u.age_group,
    u.generation,
    u.state,
    u.country,
    u.region,
    u.is_domestic,
    u.acquisition_channel,
    
    -- Return analysis
    case when oi.is_returned then oi.sale_price else 0 end as returned_value,
    case when oi.is_returned then (oi.sale_price - p.cost) else 0 end as returned_profit_impact  -- Fixed this line

from {{ ref('stg_order_items') }} as oi
inner join {{ ref('stg_products') }} as p
    on oi.product_id = p.product_id
inner join {{ ref('stg_orders') }} as o
    on oi.order_id = o.order_id
inner join {{ ref('stg_users') }} as u
    on oi.user_id = u.user_id
where not oi.has_invalid_price
  and not oi.has_missing_product
  and not p.has_invalid_cost
  and not p.has_negative_margin
  and not o.has_missing_user