{% snapshot listing_snapshot %}

{{
        config(
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='scraped_date',
          alias='snapshot_listing'
        )
    }}

select
    id,
    listing_id,
    scrape_id,
    scraped_date,
    host_id,
    host_name,
    host_since,
    host_neighbourhood,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    has_availability
from {{ ref('b_listing') }}

{% endsnapshot %}