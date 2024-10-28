{{ config(unique_key="room_type_id", alias="dim_room_type") }}

select 
    distinct room_type, 
    md5(room_type) as room_type_id,
    valid_from, 
    valid_to
from
    {{ ref("s_dim_listing") }}

    -- SELECT 
    -- room_type_id,
    -- room_type,
    -- max(valid_from) as valid_from,
    -- max(coalesce(valid_to, '9999/12/31'::timestamp)) as valid_to
    -- FROM cleaned
    -- group by room_type_id, room_type
    -- ORDER BY room_type_id, valid_from, valid_to
    
