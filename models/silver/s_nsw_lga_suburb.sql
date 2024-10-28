{{ config(unique_key="suburb_id", alias="nsw_lga_suburb") }}

select
    md5(lower(suburb_name)) as suburb_id,
    lga_name,
    suburb_name
from {{ ref("b_nsw_lga_suburb") }}
