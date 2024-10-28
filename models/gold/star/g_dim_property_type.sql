{{ config(unique_key="property_type_id", alias="dim_property_type") }}

select
    distinct property_type as property_type,
    md5(property_type) as property_type_id,
    valid_from,
    valid_to
from
    {{ ref("s_dim_listing") }}
    
