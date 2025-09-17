{{ config(
    MATERIALIZED='view',
    tags=['staging', 'products']
)}}

select
    id as product_id,
    cost,
    category,
    name as product_name,
    brand,
    retail_price,
    department,
    sku,
    distribution_center_id,
    
    -- Margin calculations
    retail_price - cost as margin_amount,
    case when retail_price > 0 then
        round((retail_price - cost) / retail_price * 100, 2)
        else 0 end as margin_percentage,
    
    -- Product categorization
    case 
        when retail_price - cost > 100 then 'Premium'
        when retail_price - cost > 50 then 'High'
        when retail_price - cost > 20 then 'Standard'
        when retail_price - cost > 5 then 'Economy'
        else 'Low'
    end as margin_tier,
    
    case
        when retail_price >= 200 then 'Luxury'
        when retail_price >= 100 then 'Premium'
        when retail_price >= 50 then 'Mid-Range'
        when retail_price >= 20 then 'Budget'
        else 'Economy'
    end as price_category,
    
    -- Category grouping (simplified)
    case 
        when lower(category) like '%apparel%' or lower(category) like '%clothing%' then 'Apparel'
        when lower(category) like '%shoe%' or lower(category) like '%footwear%' then 'Footwear'
        when lower(category) like '%accessori%' or lower(category) like '%jewelry%' then 'Accessories'
        when lower(category) like '%home%' or lower(category) like '%decor%' then 'Home & Decor'
        when lower(category) like '%sport%' or lower(category) like '%outdoor%' then 'Sports & Outdoors'
        else 'Other'
    end as category_group,
    
    -- Brand tier (based on average retail price)
    case 
        when brand in ('Calvin Klein', 'Diesel', 'Hugo Boss') then 'Premium'
        when brand in ('Adidas', 'Nike', "Levi's") then 'Popular'
        else 'Standard'
    end as brand_tier,
    
    -- Data quality flags
    case when cost <= 0 then true else false end as has_invalid_cost,
    case when retail_price <= 0 then true else false end as has_invalid_price,
    case when retail_price < cost then true else false end as has_negative_margin,
    case when cost > 5000 or retail_price > 10000 then true else false end as has_extreme_values,
    case when name is null or trim(name) = '' then true else false end as has_missing_name

from {{ source('thelook_ecommerce', 'products') }}