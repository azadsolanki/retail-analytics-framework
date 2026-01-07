{% macro test_assert_revenue_accuracy(model, revenue_column, tolerance=0.01) %}

    with source_total as (
        select sum(sale_price) as source_revenue
        from {{ source('thelook_ecommerce', 'order_items') }} oi
        join {{ source('thelook_ecommerce', 'orders') }} o
            on oi.order_id = o.order_id
        where o.status = 'Complete'
    ),
    
    model_total as (
        select sum({{ revenue_column }}) as model_revenue
        from {{ model }}
    ),
    
    variance_check as (
        select
            source_revenue,
            model_revenue,
            abs(source_revenue - model_revenue) / source_revenue as variance_pct
        from source_total
        cross join model_total
    )
    
    select *
    from variance_check
    where variance_pct > {{ tolerance }}

{% endmacro %}