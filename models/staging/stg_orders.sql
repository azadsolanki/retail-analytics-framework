{{ config(
    materialized='view',
    tags=['staging', 'orders']
) }}

select
    order_id,
    user_id,
    status as order_status,
    gender as customer_gender,
    created_at as order_date,
    returned_at,
    shipped_at,
    delivered_at,
    num_of_item as item_count,
    created_at,
    
    -- Order status flags
    case when returned_at is not null then true else false end as is_returned,
    case when shipped_at is not null then true else false end as is_shipped,
    case when delivered_at is not null then true else false end as is_delivered,
    case when status = 'Complete' then true else false end as is_complete,
    case when status = 'Cancelled' then true else false end as is_cancelled,
    
    -- Delivery timing calculations
    case when shipped_at is not null then
        date_diff(shipped_at, created_at, hour) end as hours_to_ship,
    case when delivered_at is not null and shipped_at is not null then
        date_diff(delivered_at, shipped_at, hour) end as hours_in_transit,
    case when delivered_at is not null then
        date_diff(delivered_at, created_at, hour) end as total_delivery_hours,
        
    -- Convert to days for easier analysis
    case when delivered_at is not null then
        date_diff(delivered_at, created_at, day) end as delivery_days,
        
    -- Order timing analysis
    extract(hour from created_at) as order_hour,
    extract(dayofweek from created_at) as order_day_of_week,
    extract(month from created_at) as order_month,
    extract(quarter from created_at) as order_quarter,
    extract(year from created_at) as order_year,
    
    case 
        when extract(hour from created_at) between 6 and 11 then 'Morning'
        when extract(hour from created_at) between 12 and 17 then 'Afternoon'
        when extract(hour from created_at) between 18 and 21 then 'Evening'
        else 'Night'
    end as order_time_of_day,
    
    case extract(dayofweek from created_at)
        when 1 then 'Sunday'
        when 2 then 'Monday'
        when 3 then 'Tuesday'
        when 4 then 'Wednesday'
        when 5 then 'Thursday'
        when 6 then 'Friday'
        when 7 then 'Saturday'
    end as order_day_name,
    
    case when extract(dayofweek from created_at) in (1, 7) then true else false end as is_weekend,
    
    -- Data quality checks
    case when user_id is null then true else false end as has_missing_user,
    case when num_of_item <= 0 then true else false end as has_invalid_item_count,
    case when created_at > current_timestamp() then true else false end as is_future_order

from {{ source('thelook_ecommerce', 'orders') }}
where created_at >= '{{ var("start_date") }}'
  and created_at <= '{{ var("end_date") }}'

-- Development environment filter
{% if target.name == 'dev' and var('dev', {}).get('limit_data', false) %}
    qualify row_number() over (order by created_at desc) <= {{ var('dev').max_records }}
{% endif %}
