{{ config(unique_key="id", alias="dim_listing") }}

select
    id,
    listing_id::bigint as listing_id,
    scrape_id::bigint as scrape_id,
    to_date(scraped_date, 'yyyy-MM-dd') as scraped_date,
    host_id::bigint as host_id,
    case when host_name = 'NaN' then null else host_name end::varchar(255) as host_name,
    case
        when host_since = 'NaN' or host_since is null
        then to_date('01/01/1970', 'dd/MM/yyyy')
        else to_date(host_since, 'dd/MM/yyyy')
    end as host_since,
    case
        when host_neighbourhood = 'NaN' then null else host_neighbourhood
    end::varchar(255) as host_neighbourhood,
    listing_neighbourhood::varchar(255) as listing_neighbourhood,
    property_type::varchar(255) as property_type,
    room_type::varchar(255) as room_type,
    accommodates::int as accommodates,
    has_availability::varchar(5) as has_availability,
    dbt_valid_from::timestamp as valid_from,
    dbt_valid_to::timestamp as valid_to
from {{ ref("listing_snapshot") }}
