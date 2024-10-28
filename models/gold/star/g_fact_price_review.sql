{{ config(unique_key="row_id", alias="fact_price_review") }}

with

    dim_listing as (
        select distinct
            listing_id,
            host_neighbourhood,
            listing_neighbourhood,
            property_type,
            room_type
        from {{ ref("s_dim_listing") }}
    ),

    dim_property as (
        select distinct property_type_id, property_type
        from {{ ref("g_dim_property_type") }}
    ),

    dim_room as (
        select distinct 
            room_type_id, 
            room_type 
        from {{ ref("g_dim_room_type") }}
    )

select
    -- --------------------------------------------------------------------------------
    -- DIMENSIONS: DATES AND ID'S
    fact_listing.id as row_id,
    fact_listing.scraped_date as date,
    fact_listing.host_id as host_id,
    dim_host_suburb.lga_id as host_lga_id,
    dim_host_suburb.suburb_id as host_suburb_id,
    fact_listing.listing_id as listing_id,
    dim_listing_lga.lga_id as listing_lga_id,
    dim_property.property_type_id as listing_property_type_id,
    dim_room.room_type_id as listing_room_type_id,
    -- END DIMENSIONS
    -- --------------------------------------------------------------------------------
    -- FACTS: HOST, PRICE AND REVIEWS
    fact_listing.host_is_superhost as host_is_superhost,
    fact_listing.availability_30 as listing_availability_30,
    fact_listing.price as listing_price,
    fact_listing.number_of_reviews as listing_number_of_reviews,
    fact_listing.review_scores_rating as listing_review_scores_rating,
    fact_listing.review_scores_accuracy as listing_review_scores_accuracy,
    fact_listing.review_scores_cleanliness as listing_review_scores_cleanliness,
    fact_listing.review_scores_checkin as listing_review_scores_checkin,
    fact_listing.review_scores_communication as listing_review_scores_communication,
    fact_listing.review_scores_value as listing_review_scores_value
-- END FACTS
-- --------------------------------------------------------------------------------
from {{ ref("s_fact_listing") }} fact_listing
left join dim_listing on fact_listing.listing_id = dim_listing.listing_id
-- Get gold property type id
left join dim_property on dim_listing.property_type = dim_property.property_type
-- Get gold room type id
left join dim_room on dim_listing.room_type = dim_room.room_type
-- Get gold host suburb id
left join
    {{ ref("g_dim_suburb") }} dim_host_suburb
    on lower(dim_listing.host_neighbourhood) = lower(dim_host_suburb.suburb_name)
-- Get gold listing lga id
left join
    {{ ref("g_dim_lga") }} dim_listing_lga
    on lower(dim_listing.listing_neighbourhood) = lower(dim_listing_lga.lga_name)
