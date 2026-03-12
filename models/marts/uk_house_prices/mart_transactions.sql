/*
  Full transaction fact table. Pulls from the enriched intermediate so all
  region, flag, and price-band columns are available without re-deriving them.
  One row per sale.
*/

with enriched as (

    select * from {{ ref('int_price_paid__enriched') }}

)

select
    transaction_id,
    sale_date,
    sale_year,
    sale_month,
    sale_price_gbp,
    price_band,

    -- Property
    property_type,
    build_status,
    tenure,
    ppd_category,

    -- Flags
    is_london,
    is_new_build,
    is_leasehold,
    is_additional_ppd,

    -- Geography (enriched)
    region,
    local_authority_name,
    county,
    district,
    town_city,
    locality,
    postcode,
    latitude,
    longitude,

    -- Address detail
    flat_number,
    house_number_or_name,
    street

from enriched
