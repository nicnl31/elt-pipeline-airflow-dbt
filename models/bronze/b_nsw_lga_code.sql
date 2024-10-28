{{ config(unique_key="LGA_CODE", alias="nsw_lga_code") }}

select *
from {{ source("raw", "raw_nsw_lga_code") }}
