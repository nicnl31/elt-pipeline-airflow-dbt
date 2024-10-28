{{ config(unique_key="listing_id", alias="dim_listing") }}

select 
    distinct listing_id,
    scrape_id,
    scraped_date,
    listing_neighbourhood,
    accommodates,
    has_availability,
    valid_from,
    valid_to
from {{ ref("s_dim_listing") }}
order by listing_id, valid_from, valid_to
