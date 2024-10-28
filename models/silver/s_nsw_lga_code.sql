{{ config(unique_key="lga_id", alias="nsw_lga_code") }}


select 
    lga_code::int as lga_id, 
    lga_name
from {{ ref("b_nsw_lga_code") }}
