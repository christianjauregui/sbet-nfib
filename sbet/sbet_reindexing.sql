{{ 
    config(
        materialized = "table"
    ) 
}}

with source_data as (
    -- 1. Grab technical slugs and friendly names
    select * from {{ ref('nfib_indicators') }}
),

baseline_data as (
    select 
        baseline_value as baseline_1986_value
    from {{ ref('opt_index_1986') }}
    where metric_name = 'optimism_index'
),

calc_index_rows as (
    select
        report_month,
        industry,
        employee,
        region,
        
        -- Friendly Label for Sigma
        'Small Business Optimism Index' as indicator_name,
        -- Technical Slug for backend stability
        'optimism_index' as indicator_short,

        (
            ( (SUM(indicator_value) / 10.0) + 100 ) 
            / (select baseline_1986_value from baseline_data)
        ) * 100 as indicator_value

    from source_data s
    where indicator_short in (
        'good_time_to_expand_next3m',
        'expected_business_cond_next6m',
        'past_earnings_change_last3m',
        'expected_real_sales_higher_next3m',
        'plans_to_expand_employees_next3m',
        'any_current_job_opening',
        'current_inventory_size',
        'plans_increase_inventory_next6m',
        'expected_cred_cond',
        'expected_capex_next3m-6m'
    )
    group by 1, 2, 3, 4
)

select * from source_data
union all
select * from calc_index_rows
