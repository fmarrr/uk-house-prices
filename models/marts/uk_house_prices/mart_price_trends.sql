/*
  National and regional annual price trends with YoY change.
  Grain: sale_year + region + property_type + tenure

  Window functions are applied here to the pre-aggregated monthly intermediate,
  keeping the logic readable without touching raw transactions.
*/

with monthly as (

    select * from {{ ref('int_price_paid__monthly') }}

),

annual as (

    select
        sale_year,
        region,
        property_type,
        tenure,

        sum(transaction_count)                      as annual_transaction_count,
        avg(avg_sale_price_gbp)                     as avg_sale_price_gbp,
        -- Weighted median approximation: average of monthly medians
        avg(median_sale_price_gbp)                  as avg_median_sale_price_gbp,
        sum(total_sale_value_gbp)                   as total_sale_value_gbp

    from monthly
    group by 1, 2, 3, 4

),

with_yoy as (

    select
        *,

        lag(avg_sale_price_gbp) over (
            partition by region, property_type, tenure
            order by sale_year
        )                                           as prev_year_avg_price_gbp,

        round(
            (avg_sale_price_gbp
                - lag(avg_sale_price_gbp) over (
                    partition by region, property_type, tenure
                    order by sale_year
                )
            )
            / nullif(
                lag(avg_sale_price_gbp) over (
                    partition by region, property_type, tenure
                    order by sale_year
                ), 0
            ) * 100,
            2
        )                                           as yoy_avg_price_change_pct,

        -- 3-year rolling average price
        avg(avg_sale_price_gbp) over (
            partition by region, property_type, tenure
            order by sale_year
            rows between 2 preceding and current row
        )                                           as rolling_3yr_avg_price_gbp

    from annual

)

select * from with_yoy
order by region, property_type, tenure, sale_year
