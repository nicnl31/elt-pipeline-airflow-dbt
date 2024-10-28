{{ config(unique_key="host_id", alias="dim_host") }}

select 
    distinct host_id, 
    host_name, 
    host_since, 
    host_neighbourhood, 
    valid_from, 
    valid_to
from {{ ref("s_dim_listing") }}
order by host_id, valid_from, valid_to
