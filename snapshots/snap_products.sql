{% snapshot snap_products %}

{{
    config(
        target_schema='snapshot',
        unique_key='id',
        strategy='check',
        check_cols=['cost', 'retail_price', 'category', 'brand', 'name'],
        invalidate_hard_deletes=True,
        tags=['snapshots', 'scd2']

    )
}}

select
    id as product_id,
    name as product_name,
    category,
    brand,
    cost,
    retail_price,
    department,
    sku,
    distribution_center_id

from {{ source('thelook_ecommerce', 'products') }}

{% endsnapshot %}