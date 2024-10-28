{{ config(unique_key="ID", alias="nsw_lga_suburb") }}

select {{ dbt_utils.generate_surrogate_key(["LGA_NAME", "SUBURB_NAME"]) }} as id, *
from {{ source("raw", "raw_nsw_lga_suburb") }}
