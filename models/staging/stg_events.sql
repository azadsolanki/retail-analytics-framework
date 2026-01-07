{{ config(
    MATERIALIZED='view',
    tags=['staging', 'events']
) }}

select
    id as event_id,
    user_id,
    sequence_number,
    session_id,
    created_at as event_timestamp,
    ip_address,
    city,
    state,
    postal_code,
    browser,
    traffic_source,
    uri,
    event_type,
    
    -- Event categorization
    case 
        when event_type = 'home' then 'Navigation'
        when event_type in ('department', 'category', 'brand') then 'Browse'
        when event_type = 'product' then 'Product View'
        when event_type = 'cart' then 'Add to Cart'
        when event_type = 'purchase' then 'Purchase'
        else 'Other'
    end as event_category,
    
    -- Session analysis
    extract(hour from created_at) as event_hour,
    extract(dayofweek from created_at) as event_day_of_week,
    
    -- Geographic
    case when state is not null then state else 'Unknown' end as user_state,
    case when city is not null then city else 'Unknown' end as user_city,
    
    -- Device/browser categorization
    case 
        when lower(browser) like '%chrome%' then 'Chrome'
        when lower(browser) like '%safari%' then 'Safari'
        when lower(browser) like '%firefox%' then 'Firefox'
        when lower(browser) like '%edge%' then 'Edge'
        else 'Other'
    end as browser_family,
    
    -- Data quality
    case when user_id is null then true else false end as has_missing_user,
    case when session_id is null then true else false end as has_missing_session

from {{ source('thelook_ecommerce', 'events') }}
where created_at >= '{{ var("start_date") }}'
  and created_at <= '{{ var("end_date") }}'

-- Development environment filter
{% if target.name == 'dev' and var('dev', {}).get('limit_data', false) %}
    qualify row_number() over (order by created_at desc) <= {{ var('dev').max_records }}
{% endif %}