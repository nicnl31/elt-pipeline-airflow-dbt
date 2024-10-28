{{ config(unique_key="LGA_CODE_2016", alias="g02_census_2016_nsw_lga") }}

select *
from {{ source("raw", "raw_2016census_g02_nsw_lga") }}
