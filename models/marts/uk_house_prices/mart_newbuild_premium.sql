/*
  New build price premium over established properties at street level.
  Grain: sale_year + street + postcode_sector + county + town_city + property_type

  postcode_sector is null where postcode is missing; in that case
  street + county + town_city identifies the location (same logic as
  mart_street_yearly_prices and mart_price_trends).

  Pivots avg prices by build_status then computes:
    - Absolute premium (£)
    - Premium as % of established price
*/

with enriched as (

    select * from {{ ref('int_price_paid__enriched') }}

),

classified as (

    select
        sale_year,
        sale_price_gbp,
        street,
        county,
        district,
        town_city,
        case
            when postcode is not null
             and length(trim(postcode)) > 2
            then substr(trim(postcode), 1, length(trim(postcode)) - 2)
        end                                             as postcode_sector,
        postcode is not null
            and length(trim(postcode)) > 2              as has_postcode,
        property_type,
        build_status

    from enriched
    where street is not null
    -- Only compare like-for-like tenure to avoid tenure mix skewing the premium
      and (tenure = 'Leasehold'
       or (tenure = 'Freehold' and property_type != 'Flat/Maisonette'))

),

annual_by_build_status as (

    select
        sale_year,
        street,
        county,
        district,
        town_city,
        postcode_sector,
        has_postcode,
        property_type,
        build_status,

        count(*)                                        as transaction_count,
        avg(sale_price_gbp)                             as avg_sale_price_gbp,
        approx_quantiles(sale_price_gbp, 100)[offset(50)] as median_sale_price_gbp

    from classified
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9

),

pivoted as (

    select
        sale_year,
        street,
        county,
        district,
        town_city,
        postcode_sector,
        has_postcode,
        property_type,

        max(case when build_status = 'New Build'   then avg_sale_price_gbp    end) as new_build_avg_price_gbp,
        max(case when build_status = 'Established' then avg_sale_price_gbp    end) as established_avg_price_gbp,
        max(case when build_status = 'New Build'   then median_sale_price_gbp end) as new_build_median_price_gbp,
        max(case when build_status = 'Established' then median_sale_price_gbp end) as established_median_price_gbp,
        max(case when build_status = 'New Build'   then transaction_count     end) as new_build_count,
        max(case when build_status = 'Established' then transaction_count     end) as established_count

    from annual_by_build_status
    group by 1, 2, 3, 4, 5, 6, 7, 8

)

select
    sale_year,
    street,
    county,
    district,
    town_city,
    postcode_sector,
    has_postcode,
    property_type,

    new_build_avg_price_gbp,
    established_avg_price_gbp,
    new_build_median_price_gbp,
    established_median_price_gbp,
    new_build_count,
    established_count,

    -- Premium metrics
    round(new_build_avg_price_gbp - established_avg_price_gbp, 0)
                                                        as avg_premium_gbp,

    round(
        (new_build_avg_price_gbp - established_avg_price_gbp)
        / nullif(established_avg_price_gbp, 0) * 100,
        2
    )                                                   as avg_premium_pct,

    round(new_build_median_price_gbp - established_median_price_gbp, 0)
                                                        as median_premium_gbp

from pivoted
where new_build_avg_price_gbp    is not null
  and established_avg_price_gbp  is not null
order by street, postcode_sector, county, property_type, sale_year
