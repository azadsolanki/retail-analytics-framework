{% macro calculate_rfm_scores(table_name, customer_col, date_col, revenue_col) %}

    with rfm_calc as (
        select
            {{ customer_col }},
            
            -- Recency (days since last purchase)
            date_diff(current_date(), max(date({{ date_col }})), day) as recency_days,
            
            -- Frequency (number of purchases)
            count(*) as frequency,
            
            -- Monetary (total spent)
            sum({{ revenue_col }}) as monetary
            
        from {{ table_name }}
        group by {{ customer_col }}
    ),
    
    rfm_scores as (
        select
            *,
            ntile(5) over (order by recency_days desc) as recency_score,
            ntile(5) over (order by frequency) as frequency_score,
            ntile(5) over (order by monetary) as monetary_score
        from rfm_calc
    )
    
    select
        *,
        concat(
            cast(recency_score as string),
            cast(frequency_score as string), 
            cast(monetary_score as string)
        ) as rfm_segment,
        
        case 
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champions'
            when recency_score >= 3 and frequency_score >= 4 and monetary_score >= 4 then 'Loyal Customers'
            when recency_score >= 4 and frequency_score <= 2 and monetary_score >= 3 then 'Potential Loyalists'
            when recency_score >= 4 and frequency_score <= 2 and monetary_score <= 2 then 'New Customers'
            when recency_score >= 3 and frequency_score >= 3 and monetary_score >= 3 then 'Promising'
            when recency_score <= 2 and frequency_score >= 3 and monetary_score >= 3 then 'At Risk'
            when recency_score <= 2 and frequency_score <= 2 and monetary_score >= 4 then 'Cannot Lose Them'
            else 'Others'
        end as rfm_segment_name

    from rfm_scores

{% endmacro %} <!