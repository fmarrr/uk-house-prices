{{
    config(severity='warn')
}}

/*
  A given street + postcode_district should always map to a single county.
  Rows returned indicate streets that span county boundaries or have inconsistent
  county data. Expected to find some real-world cases (streets on county borders).
  Set to warn so it surfaces the count without blocking downstream models.
*/

select
    street,
    substr(postcode, 1, 3)  as postcode_district,
    count(distinct county)  as county_count

from {{ ref('int_price_paid__enriched') }}

where street  is not null
  and postcode is not null

group by 1, 2
having count(distinct county) > 1
