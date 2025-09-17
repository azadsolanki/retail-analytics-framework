{{ config(
    MATERIALIZED='view',
    tags=['staging', 'users']
) }}

select 
    id as user_id, 
    first_name, 
    last_name, 
    concat(coalesce(first_name, ''), ' ', coalesce(last_name, '')) as full_name,
    email,
    age,
    gender,
    state,
    country, 
    latitude,
    longitude,
    traffic_source,
    created_at as registration_date,
    created_at,
    
    -- calculate demographics
    CASE 
        WHEN age BETWEEN 13 and 17 THEN 'Gen Z (13 - 17)'
        WHEN age BETWEEN 18 and 24 THEN 'Gen Z (18 - 24)'
        WHEN age BETWEEN 25 and 40 THEN 'Millennial'
        WHEN age BETWEEN 41 and 56 THEN 'Gen X'
        when age between 57 and 75 then 'Boomer'
        WHEN age > 75 THEN 'Silent'
        ELSE  'Unknown'
    END AS generation,

    CASE 
        WHEN age < 25 THEN  '18-24'
        when age < 35 then '25-34'
        when age < 45 then '35-44'
        when age < 55 then '45-54'
        when age < 65 then '55-64'
        ELSE '65+' 
    END as age_group,

    -- Geographic standardization 
    CASE 
        WHEN country = 'USA' THEN state  
        ELSE country
    END as region,

    CASE 
        WHEN country = 'USA' THEN true
        ELSE false
    END as is_domestic,

    -- Acquisition channel grouping
    case 
        when lower(traffic_source) in ('google', 'search', 'organic') then 'Organic Search'
        when lower(traffic_source) in ('facebook', 'instagram', 'social') then 'Social Media'
        when lower(traffic_source) = 'email' then 'Email'
        when lower(traffic_source) in ('youtube', 'video') then 'Video'
        when lower(traffic_source) in ('display', 'banner') then 'Display'
        else 'Other'
    end as acquisition_channel,
    
    -- Data quality flags
    case when email is null or email = '' or not regexp_contains(email, r'^[^@]+@[^@]+\.[^@]+$') 
         then true else false end as has_invalid_email,
    case when age < 13 or age > 120 then true else false end as has_invalid_age,
    case when first_name is null and last_name is null then true else false end as has_missing_name,
    case when latitude is null or longitude is null then true else false end as has_missing_coordinates

from {{ source('thelook_ecommerce', 'users') }}
where created_at >= '{{ var("start_date") }}'
  and created_at <= '{{ var("end_date") }}'

-- Development environment filter
{% if target.name == 'dev' and var('dev', {}).get('limit_data', false) %}
    qualify row_number() over (order by created_at desc) <= {{ var('dev').max_records }}
{% endif %}
