with source as (

    select * from {{ source('uk_house_prices', 'price_paid_all') }}

),

renamed as (

    select
        transaction_id,
        price                                       as sale_price_gbp,
        cast(substr(transfer_date, 1, 10) as date)      as sale_date,
        extract(year  from cast(substr(transfer_date, 1, 10) as date)) as sale_year,
        extract(month from cast(substr(transfer_date, 1, 10) as date)) as sale_month,

        -- Property details
        postcode,
        case property_type
            when 'D' then 'Detached'
            when 'S' then 'Semi-Detached'
            when 'T' then 'Terraced'
            when 'F' then 'Flat/Maisonette'
            when 'O' then 'Other'
        end                                         as property_type,
        case old_new
            when 'Y' then 'New Build'
            when 'N' then 'Established'
        end                                         as build_status,
        case duration
            when 'F' then 'Freehold'
            when 'L' then 'Leasehold'
        end                                         as tenure,

        -- Address
        nullif(saon, '')                            as flat_number,
        nullif(paon, '')                            as house_number_or_name,
        nullif(street, '')                          as street,
        nullif(locality, '')                        as locality,
        nullif(town, '')                            as town_city,
        nullif(district, '')                        as district,
        nullif(county, '')                          as county,

        -- Metadata
        case ppd_category
            when 'A' then 'Standard'
            when 'B' then 'Additional'
        end                                         as ppd_category,
        record_status

    from source
    where record_status != 'D'  -- exclude deleted records

)

select * from renamed
