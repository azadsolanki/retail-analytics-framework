{{config(
    MATERIALIZED='table',
    tags=['reports', 'cohorts']
)}}

with customer_orders as (
    select
        user_id,
        date_trunc(date(order_date), month) as order_month,
        order_total,
        row_number() over (partition by user_id order by order_date) as order_sequence
    from {{ ref('fct_orders') }}
    where is_complete
),

cohort_data as (
    select
        user_id,
        min(order_month) over (partition by user_id) as cohort_month,
        order_month,
        order_total,
        date_diff(order_month, min(order_month) over (partition by user_id), month) as period_number
    from customer_orders
),

cohort_table as (
    select
        cohort_month,
        period_number,
        count(distinct user_id) as customers,
        sum(order_total) as revenue,
        avg(order_total) as avg_revenue_per_customer,
        count(*) as total_orders
    from cohort_data
    group by 1, 2
),

cohort_sizes as (
    select
        cohort_month,
        count(distinct user_id) as cohort_size,
        sum(order_total) as cohort_initial_revenue,
        avg(order_total) as cohort_avg_initial_order
    from cohort_data
    where period_number = 0
    group by 1
)

select
    ct.cohort_month,
    ct.period_number,
    cs.cohort_size,
    ct.customers,
    ct.revenue,
    ct.total_orders,
    ct.avg_revenue_per_customer,
    cs.cohort_initial_revenue,
    cs.cohort_avg_initial_order,
    
    -- Retention metrics
    round(ct.customers * 100.0 / cs.cohort_size, 2) as retention_rate,
    round(ct.revenue / cs.cohort_initial_revenue, 2) as revenue_retention_multiple,
    round(ct.total_orders * 1.0 / ct.customers, 2) as avg_orders_per_retained_customer,
    
    -- Period-over-period analysis
    lag(ct.customers) over (
        partition by ct.cohort_month 
        order by ct.period_number
    ) as previous_period_customers,
    
    round(ct.customers * 100.0 / lag(ct.customers) over (
        partition by ct.cohort_month 
        order by ct.period_number
    ), 2) as period_retention_rate,
    
    -- Cohort quality indicators
    case 
        when ct.period_number = 0 then 'Initial'
        when ct.period_number = 1 and retention_rate >= 30 then 'Strong Month 1'
        when ct.period_number = 1 and retention_rate >= 20 then 'Good Month 1'
        when ct.period_number = 1 then 'Weak Month 1'
        when ct.period_number = 6 and retention_rate >= 15 then 'Strong Month 6'
        when ct.period_number = 6 and retention_rate >= 10 then 'Good Month 6'
        when ct.period_number = 6 then 'Weak Month 6'
        when ct.period_number = 12 and retention_rate >= 10 then 'Strong Month 12'
        when ct.period_number = 12 and retention_rate >= 5 then 'Good Month 12'
        when ct.period_number = 12 then 'Weak Month 12'
        else 'Standard'
    end as cohort_quality,
    
    current_timestamp() as dbt_updated_at

from cohort_table ct
left join cohort_sizes cs
    on ct.cohort_month = cs.cohort_month
order by ct.cohort_month, ct.period_number
