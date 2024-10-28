{{ config(unique_key="id", alias="listing") }}

select 
    {{ dbt_utils.generate_surrogate_key(["LISTING_ID", "SCRAPED_DATE"]) }} as id, 
    *
from {{ source("raw", "raw_listing") }}