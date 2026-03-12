/*
  Aggregates enriched transactions to a monthly grain.
  Grain: sale_year + sale_month + region + county + property_type + build_status + tenure

  Used as the foundation for mart_price_trends and mart_market_activity,
  keeping window functions and joins out of the marts.
*/

with enriched as (

    select * from {{ ref('int_price_paid__enriched') }}

),

monthly as (

    select
        sale_year,
        sale_month,

        -- First day of the month — useful for time-series charting
        date_from_parts(sale_year, sale_month, 1)   as month_start_date,

        region,
        county,
        property_type,
        build_status,
        tenure,

        count(*)                                    as transaction_count,
        avg(sale_price_gbp)                         as avg_sale_price_gbp,
        median(sale_price_gbp)                      as median_sale_price_gbp,
        min(sale_price_gbp)                         as min_sale_price_gbp,
        max(sale_price_gbp)                         as max_sale_price_gbp,
        sum(sale_price_gbp)                         as total_sale_value_gbp,

        -- New build split
        count_if(is_new_build)                      as new_build_count,
        count_if(not is_new_build)                  as established_count,

        -- London split
        count_if(is_london)                         as london_transaction_count

    from enriched
    group by 1, 2, 3, 4, 5, 6, 7, 8

)

select * from monthly
