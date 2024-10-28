{{
    config(
        unique_key='lga_id',
        alias='dim_lga'
    )
}}

select
    distinct lga_id,
    lga_name
from {{ ref('s_nsw_lga_code') }}
