{{config(
    MATERIALIZED='table',
    tags=['reports', 'products']
) }}


with product_analysis as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.category_group,
        p.brand,
        p.brand_tier,
        p.department,
        p.price_category,
        p.product_cost,
        p.retail_price,
        p.margin_percentage,
        p.performance_tier,
        p.inventory_status,
        
        -- Sales metrics
        p.total_revenue,
        p.total_units_sold,
        p.total_orders,
        p.unique_customers,
        p.return_rate,
        p.revenue_90d,
        p.units_sold_90d,
        
        -- Calculated metrics
        p.revenue_per_customer,
        p.avg_units_per_order,
        p.geographic_reach_score,
        
        -- Ranking within category
        row_number() over (
            partition by p.category_group 
            order by p.total_revenue desc
        ) as revenue_rank_in_category,
        
        row_number() over (
            partition by p.category_group 
            order by p.total_units_sold desc
        ) as units_rank_in_category,
        
        -- Overall rankings
        row_number() over (order by p.total_revenue desc) as overall_revenue_rank,
        row_number() over (order by p.return_rate asc) as return_rate_rank,
        
        -- Category performance
        sum(p.total_revenue) over (partition by p.category_group) as category_total_revenue,
        count(*) over (partition by p.category_group) as products_in_category

    from {{ ref('dim_products') }} p
    where p.total_revenue > 0
)

select
    *,
    
    -- Market share within category
    round(total_revenue * 100.0 / category_total_revenue, 2) as category_revenue_share,
    
    -- Performance indicators
    case 
        when revenue_rank_in_category <= 3 then 'Top 3 in Category'
        when revenue_rank_in_category <= 10 then 'Top 10 in Category'
        when revenue_rank_in_category <= products_in_category * 0.2 then 'Top 20% in Category'
        else 'Bottom 80% in Category'
    end as category_performance_tier,
    
    case 
        when overall_revenue_rank <= 100 then 'Top 100 Overall'
        when overall_revenue_rank <= 500 then 'Top 500 Overall'
        else 'Long Tail'
    end as overall_performance_tier,
    
    -- Growth potential
    case 
        when revenue_90d > total_revenue * 0.5 then 'High Recent Activity'
        when revenue_90d > total_revenue * 0.2 then 'Moderate Recent Activity'
        when revenue_90d > 0 then 'Low Recent Activity'
        else 'No Recent Activity'
    end as recent_activity_level,
    
    -- Recommendation flags
    case when return_rate > 0.1 then true else false end as high_return_flag,
    case when margin_percentage < 20 then true else false end as low_margin_flag,
    case when revenue_90d = 0 and total_revenue > 1000 then true else false end as discontinued_flag,
    
    current_timestamp() as dbt_updated_at

from product_analysis
order by total_revenue desc
