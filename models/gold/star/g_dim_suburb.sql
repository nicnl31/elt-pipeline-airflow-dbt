{{ config(unique_key="suburb_id", alias="dim_suburb") }}


select
    distinct suburb.suburb_id as suburb_id,
    code.lga_id as lga_id,
    suburb.suburb_name as suburb_name
from {{ ref("s_nsw_lga_code") }} code
inner join
    {{ ref("s_nsw_lga_suburb") }} suburb
    on lower(code.lga_name) = lower(suburb.lga_name)
