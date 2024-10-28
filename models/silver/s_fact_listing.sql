{{ config(unique_key="id", alias="fact_listing") }}

select
    id,
    listing_id::bigint as listing_id,
    to_date(scraped_date, 'yyyy-MM-dd') as scraped_date,
    host_id::bigint as host_id,
    host_is_superhost::varchar(5) as host_is_superhost,
    price::decimal(10, 2) as price,
    availability_30::decimal(3, 1) as availability_30,
    number_of_reviews::decimal(10, 1) as number_of_reviews,
    case
        when review_scores_rating = 'NaN' then null else review_scores_rating
    end::decimal(5, 1) as review_scores_rating,
    case
        when review_scores_accuracy = 'NaN' then null else review_scores_accuracy
    end::decimal(5, 1) as review_scores_accuracy,
    case
        when review_scores_cleanliness = 'NaN' then null else review_scores_cleanliness
    end::decimal(5, 1) as review_scores_cleanliness,
    case
        when review_scores_checkin = 'NaN' then null else review_scores_checkin
    end::decimal(5, 1) as review_scores_checkin,
    case
        when review_scores_communication = 'NaN'
        then null
        else review_scores_communication
    end::decimal(5, 1) as review_scores_communication,
    case
        when review_scores_value = 'NaN' then null else review_scores_value
    end::decimal(5, 1) as review_scores_value
from {{ ref("b_listing") }}
