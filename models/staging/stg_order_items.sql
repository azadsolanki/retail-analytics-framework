{{ config(
    MATERIALIZED='view', 
    tags=['staging', 'order_items']
) }}

select
    id as order_item_id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    status as item_status,
    created_at as order_item_date,
    shipped_at as item_shipped_date,
    delivered_at as item_delivered_date,
    returned_at as item_returned_date,
    sale_price,
    
    -- Quantity handling (TheLook assumes 1 item per line)
    1 as quantity,
    sale_price as line_total,
    0 as discount_amount,
    
    -- Item status flags
    case when returned_at is not null then true else false end as is_returned,
    case when shipped_at is not null then true else false end as is_shipped,
    case when delivered_at is not null then true else false end as is_delivered,
    case when status = 'Complete' then true else false end as is_complete,
    case when status = 'Cancelled' then true else false end as is_cancelled,
    
    -- Timing analysis
    case when shipped_at is not null then
        date_diff(shipped_at, created_at, day) end as days_to_ship_item,
    case when delivered_at is not null and shipped_at is not null then
        date_diff(delivered_at, shipped_at, day) end as days_in_transit_item,
        
    created_at,
    
    -- Data quality flags
    case when sale_price <= 0 then true else false end as has_invalid_price,
    case when sale_price > 10000 then true else false end as has_extreme_price,
    case when product_id is null then true else false end as has_missing_product,
    case when user_id is null then true else false end as has_missing_user

from {{ source('thelook_ecommerce', 'order_items') }}
where created_at >= '{{ var("start_date") }}'
  and created_at <= '{{ var("end_date") }}'

-- Development environment filter
{% if target.name == 'dev' and var('dev', {}).get('limit_data', false) %}
    qualify row_number() over (order by created_at desc) <= {{ var('dev').max_records }}
{% endif %}