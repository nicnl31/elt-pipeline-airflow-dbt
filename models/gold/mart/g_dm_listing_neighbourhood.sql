{{ config(alias="dm_listing_neighbourhood") }}

with

    dim_listing as (
        select distinct listing_id, listing_neighbourhood, has_availability
        from {{ ref("g_dim_listing") }}
    ),

    data as (
        select
            f.listing_id,
            f.host_id,
            dl.listing_neighbourhood,
            date_trunc('MONTH', f.date)::date as month_year,
            f.listing_availability_30 as availability_30,
            f.listing_price as price,
            f.listing_number_of_reviews as number_of_reviews,
            f.listing_review_scores_rating as review_scores_rating,
            case
                when dl.has_availability = 't' then 1.0 else 0.0
            end as has_availability,
            lag(case when dl.has_availability = 't' then 1.0 else 0.0 end) over (
                partition by f.listing_id order by date_trunc('MONTH', f.date)::date
            ) as has_availability_last_month,
            case
                when f.host_is_superhost = 't' then 1.0 else 0.0
            end as host_is_superhost
        from {{ ref("g_fact_price_review") }} as f
        left join dim_listing as dl on f.listing_id = dl.listing_id
    )

select
    listing_neighbourhood,
    month_year,
    sum(has_availability * (30 - availability_30))::int as total_number_of_stays,
    round(sum(has_availability) / count(*) * 100, 2) as pct_active_listing_rate,
    min(has_availability * price) as min_active_listing_price,
    max(has_availability * price) as max_active_listing_price,
    percentile_cont(0.5) within group (
        order by has_availability * price
    ) as median_active_listing_price,
    round(avg(has_availability * price), 2) as avg_active_listing_price,
    count(distinct host_id) as num_distinct_hosts,
    round(
        sum(host_is_superhost) / count(distinct host_id) * 100, 2
    ) as pct_superhost_rate,
    round(
        avg(coalesce(has_availability * review_scores_rating, 0.0)), 2
    ) as avg_review_scores_rating_active_listing,
    case
        when month_year = (select min(month_year) from data)
        then 0.0
        when sum(coalesce(has_availability_last_month, 0.0)) = 0.0
        then 0.0
        else
            round(
                (
                    sum(has_availability)
                    - sum(coalesce(has_availability_last_month, 0.0))
                )
                / sum(coalesce(has_availability_last_month, 0.0))
                * 100,
                2
            )
    end as pct_change_active_listing,
    case
        when month_year = (select min(month_year) from data)
        then 0.0
        when sum(coalesce(1 - has_availability_last_month, 0.0)) = 0.0
        then 0.0
        else
            round(
                (
                    sum(1 - has_availability)
                    - sum(coalesce(1 - has_availability_last_month, 0.0))
                )
                / sum(coalesce(1 - has_availability_last_month, 0.0))
                * 100,
                2
            )
    end as pct_change_inactive_listing,
    round(
        avg(has_availability * (30 - availability_30) * price), 2
    ) as avg_estimated_revenue_per_active_listing
from data
group by listing_neighbourhood, month_year
order by listing_neighbourhood, month_year
