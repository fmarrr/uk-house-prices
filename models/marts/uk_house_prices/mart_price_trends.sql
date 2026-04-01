/*
  Year-on-year price trends at street level with 3-year rolling average.
  Built on top of mart_street_yearly_prices so gap-fill logic is not duplicated.

  Grain: sale_year + street + postcode_sector + county
  - postcode_sector is null for streets with missing postcodes;
    in that case street + county + town_city identifies the location.
  - YoY is calculated using the forward-filled price so gap years
    produce a flat (0%) change rather than a spike from zero.
*/

with street_prices as (

    select * from {{ ref('mart_street_yearly_prices') }}

),

with_yoy as (

    select
        sale_year,
        street,
        county,
        district,
        town_city,
        postcode_sector,
        has_postcode,
        transaction_count,

        avg_sale_price_gbp,
        avg_sale_price_gbp_filled,
        median_sale_price_gbp,
        median_sale_price_gbp_filled,

        -- Previous year filled price (for reference)
        lag(avg_sale_price_gbp_filled) over (
            partition by street, postcode_sector, county
            order by sale_year
        )                                               as prev_year_avg_price_gbp,

        -- YoY % change (uses filled price to avoid distortion from gap years)
        round(
            (
                avg_sale_price_gbp_filled
                - lag(avg_sale_price_gbp_filled) over (
                    partition by street, postcode_sector, county
                    order by sale_year
                )
            )
            / nullif(
                lag(avg_sale_price_gbp_filled) over (
                    partition by street, postcode_sector, county
                    order by sale_year
                ), 0
            ) * 100,
            2
        )                                               as yoy_avg_price_change_pct,

        -- 3-year rolling average (smooths short-term noise)
        round(
            avg(avg_sale_price_gbp_filled) over (
                partition by street, postcode_sector, county
                order by sale_year
                rows between 2 preceding and current row
            ),
            0
        )                                               as rolling_3yr_avg_price_gbp

    from street_prices

)

select *
from with_yoy
order by street, postcode_sector, county, sale_year
