{{ config(alias="dm_host_neighbourhood") }}

with

    dim_listing as (
        select distinct
            listing_id, listing_neighbourhood, accommodates, has_availability
        from {{ ref("g_dim_listing") }}
    ),

    dim_host as (
        select distinct host_id, host_neighbourhood from {{ ref("g_dim_host") }}
    ),

    lga_suburb as (
        select distinct
            dim_lga.lga_name as lga_name, dim_suburb.suburb_name as suburb_name
        from {{ ref("g_dim_lga") }} dim_lga
        left join
            {{ ref("g_dim_suburb") }} dim_suburb on dim_lga.lga_id = dim_suburb.lga_id
    ),

    data as (
        select
            fact.listing_id as listing_id,
            fact.host_id as host_id,
            lga_suburb.lga_name as host_neighbourhood_lga,
            date_trunc('MONTH', fact.date)::date as month_year,
            fact.listing_availability_30 as availability_30,
            fact.listing_price as price,
            case
                when dim_listing.has_availability = 't' then 1.0 else 0.0
            end as has_availability
        from {{ ref("g_fact_price_review") }} fact
        left join dim_listing on fact.listing_id = dim_listing.listing_id
        left join dim_host on fact.host_id = dim_host.host_id
        left join
            lga_suburb
            on lower(dim_host.host_neighbourhood) = lower(lga_suburb.suburb_name)
        where dim_host.host_neighbourhood is not null
    )

select
    host_neighbourhood_lga,
    month_year,
    count(distinct host_id) as num_distinct_hosts,
    sum(has_availability * (30 - availability_30) * price) as estimated_revenue,
    round(
        sum(has_availability * (30 - availability_30) * price)
        / count(distinct host_id),
        2
    ) as estimated_revenue_per_host
from data
group by host_neighbourhood_lga, month_year
order by host_neighbourhood_lga, month_year
