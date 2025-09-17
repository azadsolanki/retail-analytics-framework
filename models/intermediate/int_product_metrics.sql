{{ config(
    MATERIALIZED='ephemeral',
    tags=['intermediate']
) }}

with product_sales as (
    select
        product_id,
        
        -- Sales volume
        count(*) as total_units_sold,
        count(distinct order_id) as total_orders,
        count(distinct user_id) as unique_customers,
        
        -- Revenue metrics
        sum(line_total) as total_revenue,
        sum(item_gross_profit) as total_gross_profit,
        avg(unit_price) as avg_selling_price,
        avg(item_gross_margin_pct) as avg_gross_margin_pct,
        
        -- Return analysis
        count(case when is_returned then 1 end) as units_returned,
        sum(returned_value) as total_returned_revenue,
        
        -- Timing
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        count(distinct date(order_date)) as active_sales_days,
        
        -- Customer segments
        count(case when age_group = '25-34' then 1 end) as sales_to_millennials,
        count(case when age_group = '35-44' then 1 end) as sales_to_gen_x,
        count(case when is_domestic then 1 end) as domestic_sales,
        
        -- Geographic distribution
        count(distinct state) as states_sold_in,
        count(distinct region) as regions_sold_in,
        
        -- Seasonal patterns
        count(case when extract(quarter from order_date) = 1 then 1 end) as q1_sales,
        count(case when extract(quarter from order_date) = 2 then 1 end) as q2_sales,
        count(case when extract(quarter from order_date) = 3 then 1 end) as q3_sales,
        count(case when extract(quarter from order_date) = 4 then 1 end) as q4_sales,
        
        -- Recent performance (last 90 days)
        sum(case when date(order_date) >= date_sub(current_date(), interval 90 day) 
            then line_total else 0 end) as revenue_90d,
        count(case when date(order_date) >= date_sub(current_date(), interval 90 day) 
            then 1 end) as units_sold_90d

    from {{ ref('int_order_items_enriched') }}
    where order_complete
    group by product_id
)

select
    *,
    
    -- Performance calculations
    case when total_units_sold > 0 then
        round(units_returned * 100.0 / total_units_sold, 2)
        else 0 end as return_rate,
        
    case when total_orders > 0 then
        round(total_units_sold * 1.0 / total_orders, 2)
        else 0 end as avg_units_per_order,
        
    case when unique_customers > 0 then
        round(total_revenue / unique_customers, 2)
        else 0 end as revenue_per_customer,
    
    -- Geographic reach
    states_sold_in + regions_sold_in as geographic_reach_score,
    
    -- Seasonality index (Q4 typically highest for retail)
    case when (q1_sales + q2_sales + q3_sales + q4_sales) > 0 then
        round(q4_sales * 4.0 / (q1_sales + q2_sales + q3_sales + q4_sales), 2)
        else 0 end as q4_seasonality_index,
    
    -- Performance status
    case 
        when revenue_90d > 5000 then 'Hot'
        when revenue_90d > 1000 then 'Warm'
        when revenue_90d > 100 then 'Cool'
        when revenue_90d > 0 then 'Cold'
        else 'No Recent Sales'
    end as recent_performance_status,
    
    -- Customer concentration
    case when total_orders > 0 then
        round(unique_customers * 100.0 / total_orders, 2)
        else 0 end as customer_diversity_pct

from product_sales
