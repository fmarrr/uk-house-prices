{{
    config(
        materialized = 'incremental',
        unique_key   = ['month_start_date', 'region', 'property_type'],
        on_schema_change = 'sync_all_columns'
    )
}}

/*
  Monthly market activity with rolling volume metrics.
  Incremental on month_start_date so new months append without full refresh.
  Grain: month_start_date + region + property_type
*/

with monthly as (

    select * from {{ ref('int_price_paid__monthly') }}

    {% if is_incremental() %}
        -- Only process months not yet in the target table
        where month_start_date > (select max(month_start_date) from {{ this }})
    {% endif %}

),

with_rolling as (

    select
        month_start_date,
        sale_year,
        sale_month,
        region,
        property_type,

        transaction_count,
        avg_sale_price_gbp,
        median_sale_price_gbp,
        total_sale_value_gbp,
        new_build_count,
        established_count,

        -- 12-month rolling transaction volume
        sum(transaction_count) over (
            partition by region, property_type
            order by month_start_date
            rows between 11 preceding and current row
        )                                           as rolling_12m_transaction_count,

        -- 3-month rolling average price (smooths seasonal noise)
        avg(avg_sale_price_gbp) over (
            partition by region, property_type
            order by month_start_date
            rows between 2 preceding and current row
        )                                           as rolling_3m_avg_price_gbp,

        -- Month-on-month volume change
        lag(transaction_count) over (
            partition by region, property_type
            order by month_start_date
        )                                           as prev_month_transaction_count

    from monthly

)

select
    *,
    round(
        (transaction_count - prev_month_transaction_count)
        / nullif(prev_month_transaction_count, 0) * 100,
        2
    )                                               as mom_volume_change_pct

from with_rolling
