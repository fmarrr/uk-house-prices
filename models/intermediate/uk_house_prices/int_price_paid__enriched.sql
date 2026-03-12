/*
  Enriches each transaction with derived flags and price band.
  Note: postcode lookup (region, local_authority_name, lat/lon) is not yet loaded.
  Those columns default to null/'Unknown' until HOUSE_PRICES.RAW.POSTCODE_LOOKUP is available.
*/

with price_paid as (

    select * from {{ ref('stg_uk_house_prices__price_paid') }}

),

enriched as (

    select
        transaction_id,
        sale_date,
        sale_year,
        sale_month,
        sale_price_gbp,

        -- Property
        property_type,
        build_status,
        tenure,
        ppd_category,

        -- Address (raw)
        postcode,
        flat_number,
        house_number_or_name,
        street,
        locality,
        town_city,
        district,
        county,

        -- Postcode lookup fields (unavailable until table is loaded)
        'Unknown'   as region,
        null        as local_authority_name,
        null        as latitude,
        null        as longitude,

        -- Derived flags
        county = 'GREATER LONDON'              as is_london,
        build_status = 'New Build'             as is_new_build,
        tenure = 'Leasehold'                   as is_leasehold,
        ppd_category = 'Additional'            as is_additional_ppd,

        -- Price band
        case
            when sale_price_gbp <  100000  then 'Under £100k'
            when sale_price_gbp <  250000  then '£100k–£250k'
            when sale_price_gbp <  500000  then '£250k–£500k'
            when sale_price_gbp < 1000000  then '£500k–£1m'
            else                               'Over £1m'
        end                                    as price_band

    from price_paid

)

select * from enriched
