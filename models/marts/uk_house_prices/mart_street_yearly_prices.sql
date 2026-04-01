/*
  Yearly average sale price per street, searchable by street name or postcode sector.
  Grain: sale_year + street + postcode_sector + county

  postcode_sector = all characters except the last 2 (e.g. 'SW1A 1AA' → 'SW1A 1').
  For records where postcode is missing or invalid, postcode_sector is null and
  county/district/town_city act as the area identifier — these rows are still
  findable by street name search.

  has_postcode = false flags rows sourced from missing-postcode transactions.

  Gap years (no sales on a street in a given year) are forward-filled using
  the most recent known average so trend lines have no zero-value breaks.
  transaction_count = 0 and raw price columns = null for filled gap rows.
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
        -- Derive postcode sector where postcode is valid; null otherwise
        case
            when postcode is not null
             and length(trim(postcode)) > 2
            then substr(trim(postcode), 1, length(trim(postcode)) - 2)
        end                                             as postcode_sector,
        postcode is not null
            and length(trim(postcode)) > 2              as has_postcode

    from enriched
    where street is not null

),

yearly as (

    select
        sale_year,
        street,
        county,
        district,
        town_city,
        postcode_sector,
        has_postcode,

        count(*)                                        as transaction_count,
        avg(sale_price_gbp)                             as avg_sale_price_gbp,
        approx_quantiles(sale_price_gbp, 100)[offset(50)] as median_sale_price_gbp,
        min(sale_price_gbp)                             as min_sale_price_gbp,
        max(sale_price_gbp)                             as max_sale_price_gbp,
        sum(sale_price_gbp)                             as total_sale_value_gbp

    from classified
    group by 1, 2, 3, 4, 5, 6, 7

),

year_spine as (

    select distinct sale_year from yearly

),

streets as (

    select distinct
        street,
        county,
        district,
        town_city,
        postcode_sector,
        has_postcode
    from yearly

),

spine as (

    -- Every street × area × every year combination (includes gap years)
    select s.street, s.county, s.district, s.town_city,
           s.postcode_sector, s.has_postcode, y.sale_year
    from streets s
    cross join year_spine y

),

with_gaps as (

    select
        sp.sale_year,
        sp.street,
        sp.county,
        sp.district,
        sp.town_city,
        sp.postcode_sector,
        sp.has_postcode,
        coalesce(y.transaction_count, 0)                as transaction_count,
        y.avg_sale_price_gbp,
        y.median_sale_price_gbp,
        y.min_sale_price_gbp,
        y.max_sale_price_gbp,
        y.total_sale_value_gbp

    from spine sp
    left join yearly y
        on  sp.street           = y.street
        -- null-safe join: match null postcode_sector to null, and known sector to known sector
        and sp.postcode_sector  is not distinct from y.postcode_sector
        and sp.county           is not distinct from y.county
        and sp.sale_year        = y.sale_year

),

filled as (

    select
        sale_year,
        street,
        county,
        district,
        town_city,
        postcode_sector,
        has_postcode,
        transaction_count,

        -- Actual values for years with sales (null for gap years)
        avg_sale_price_gbp,
        median_sale_price_gbp,
        min_sale_price_gbp,
        max_sale_price_gbp,
        total_sale_value_gbp,

        -- Forward-filled: carry last known price into gap years
        last_value(avg_sale_price_gbp ignore nulls) over (
            partition by street, postcode_sector, county
            order by sale_year
            rows between unbounded preceding and current row
        )                                               as avg_sale_price_gbp_filled,

        last_value(median_sale_price_gbp ignore nulls) over (
            partition by street, postcode_sector, county
            order by sale_year
            rows between unbounded preceding and current row
        )                                               as median_sale_price_gbp_filled

    from with_gaps

)

select *
from filled
-- Exclude streets where no sale has ever occurred before the gap (nothing to fill from)
where avg_sale_price_gbp_filled is not null
order by street, postcode_sector, county, sale_year
