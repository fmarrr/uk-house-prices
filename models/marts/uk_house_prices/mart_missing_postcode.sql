/*
  Data quality model: transactions where street and county are present
  but postcode is missing or invalid (null, empty, or too short to derive a sector).

  Use this to assess coverage gaps before joining to postcode-based analyses
  such as mart_street_yearly_prices.
*/

with enriched as (

    select * from {{ ref('int_price_paid__enriched') }}

),

missing as (

    select
        street,
        county,
        district,
        town_city,
        locality,

        -- What we have vs what is missing
        postcode,
        case
            when postcode is null        then 'null'
            when trim(postcode) = ''     then 'empty_string'
            when length(postcode) <= 2   then 'too_short'
        end                                             as missing_reason,

        count(*)                                        as transaction_count,
        min(sale_date)                                  as earliest_sale,
        max(sale_date)                                  as latest_sale,
        avg(sale_price_gbp)                             as avg_sale_price_gbp,
        min(sale_price_gbp)                             as min_sale_price_gbp,
        max(sale_price_gbp)                             as max_sale_price_gbp

    from enriched
    where street  is not null
      and county  is not null
      and (
          postcode is null
          or trim(postcode) = ''
          or length(postcode) <= 2
      )
    group by 1, 2, 3, 4, 5, 6, 7

)

select *
from missing
order by county, street, missing_reason
