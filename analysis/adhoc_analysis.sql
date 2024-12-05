-- PART 4: AD-HOC ANALYSIS


--------------------------------------------------------------------------------------
-- QUESTION A
-- What are the demographic differences (e.g., age group distribution, household size)
-- between the top 3 performing and lowest 3 performing LGAs based on estimated
-- revenue per active listing over the last 12 months?
--------------------------------------------------------------------------------------

create or replace view public.q4a
as
  (select dln.listing_neighbourhood,
          sum(dln.avg_estimated_revenue_per_active_listing) as
          sum_revenue_per_active_listing
   from   gold.dm_listing_neighbourhood dln
   group  by dln.listing_neighbourhood);

create or replace view public.q4a1
as
  (select qa.listing_neighbourhood,
          qa.sum_revenue_per_active_listing,
          dc.total_population_m,
          dc.total_population_f,
          dc.age_0_4_year_p,
          dc.age_5_14_year_p,
          dc.age_15_19_year_p,
          dc.age_20_24_year_p,
          dc.age_25_34_year_p,
          dc.age_35_44_year_p,
          dc.age_45_54_year_p,
          dc.age_55_64_year_p,
          dc.age_65_74_year_p,
          dc.age_75_84_year_p,
          dc.age_85_over_p,
          dc.indigenous_total_p,
          dc.indigenous_aboriginal_m,
          dc.indigenous_torres_strait_p,
          dc.indigenous_both_aboriginal_torres_strait_p,
          dc.birthplace_australia_p,
          dc.birthplace_elsewhere_p,
          dc.australian_citizen_p,
          dc.language_spoken_home_english_only_p,
          dc.language_spoken_home_other_p,
          dc.age_attend_education_institution_0_4_p,
          dc.age_attend_education_institution_5_14_p,
          dc.age_attend_education_institution_15_19_p,
          dc.age_attend_education_institution_20_24_p,
          dc.age_attend_education_institution_25_over_p,
          dc.school_year_12_completed_p,
          dc.school_did_not_attend_school_p,
          dc.count_private_dwellings_p,
          dc.count_other_dwellings_p
   from   public.q4a qa
          left join gold.dim_lga dl
                 on qa.listing_neighbourhood = dl.lga_name
          left join gold.dim_census dc
                 on dl.lga_id = dc.lga_id);

-- Blacktown, Fairfield and Canterbury-Bankstown are the LGAs with the lowest total 
-- revenue per active listing.
-- Mosman, Northern Beaches and Woollahra are the LGAs with the highest total revenue
-- per active listing.
(select *
 from   public.q4a1 qa
 order  by sum_revenue_per_active_listing
 limit  3)
union
(select *
 from   public.q4a1 qa
 order  by sum_revenue_per_active_listing desc
 limit  3)
order  by sum_revenue_per_active_listing desc;


--------------------------------------------------------------------------------------
-- QUESTION B
-- Is there a correlation between the median age of a neighbourhood (from Census data)
-- and the revenue generated per active listing in that neighbourhood?
--------------------------------------------------------------------------------------
-- Create base data
create or replace view q4b
as
  (select dln.listing_neighbourhood                                   as
          neighbourhood,
          avg(lcd.median_age)                                         as
             median_age,
          round(avg(dln.avg_estimated_revenue_per_active_listing), 2) as
          estimated_revenue_per_active_listing
   from   gold.dm_listing_neighbourhood dln
          left join (select dl.lga_name   as listing_neighbourhood,
                            dc.median_age as median_age
                     from   gold.dim_census as dc
                            left join gold.dim_lga as dl
                                   on dc.lga_id = dl.lga_id) lcd
                 on dln.listing_neighbourhood = lcd.listing_neighbourhood
   group  by dln.listing_neighbourhood);

-- Correlation
select corr(median_age, estimated_revenue_per_active_listing) as correlation
from   public.q4b;

-- CORRELATION RESULT
---------------------+
-- correlation       |
---------------------+
-- 0.6431614010622034|
---------------------+


--------------------------------------------------------------------------------------
-- QUESTION C
-- What will be the best type of listing (property type, room type and accommodates)
-- for the top 5 “listing_neighbourhood” (in terms of estimated revenue per active 
-- listing) to have the highest number of stays?
--------------------------------------------------------------------------------------

-- Create base data
create or replace view public.q4c
as
  (select dln.listing_neighbourhood                                   as
          neighbourhood,
          dpt.property_type,
          dpt.room_type,
          dpt.accommodates,
          round(avg(dpt.avg_estimated_revenue_per_active_listing), 2) as
          avg_estimated_revenue_per_active_listing
   from   gold.dm_listing_neighbourhood as dln
          left join gold.dm_property_type as dpt
                 on dln.month_year = dpt.month_year
   group  by dln.listing_neighbourhood,
             dpt.property_type,
             dpt.room_type,
             dpt.accommodates);

-- Filter the top 5 neighbourhoods with the highest average revenue per active listing
create or replace view public.q4c1
as
  (select neighbourhood,
          round(avg(avg_estimated_revenue_per_active_listing), 2) as
          avg_estimated_revenue_per_active_listing
   from   public.q4c
   group  by neighbourhood
   order  by avg_estimated_revenue_per_active_listing desc
   limit  5);

-- RESULT: top 5 neighbourhoods with the highest average revenue per active listing
----------------+----------------------------------------+
--|neighbourhood|avg_estimated_revenue_per_active_listing|
--|-------------+----------------------------------------+
--|Blacktown    |                   5541.2245632183908046|
--|Burwood      |                   5541.2245632183908046|
--|Camden       |                   5541.2245632183908046|
--|Campbelltown |                   5541.2245632183908046|
--|Bayside      |                   5541.2245632183908046|
----------------+----------------------------------------+
-- END RESULT
  
-- Filter the max average revenue per active listing for the top 5 neighbourhoods
create or replace view public.q4c2
as
  (select neighbourhood,
          max(avg_estimated_revenue_per_active_listing) as
          max_avg_estimated_revenue_per_active_listing
   from   public.q4c
   where  neighbourhood in (select neighbourhood
                            from   public.q4c1)
   group  by neighbourhood);

-- RESULT: max average revenue per active listing for the top 5 neighbourhoods
----------------+--------------------------------------------+
--|neighbourhood|max_avg_estimated_revenue_per_active_listing|
--|-------------+--------------------------------------------+
--|Bayside      |                                   484928.40|
--|Blacktown    |                                   484928.40|
--|Burwood      |                                   484928.40|
--|Camden       |                                   484928.40|
--|Campbelltown |                                   484928.40|
----------------+--------------------------------------------+
-- END RESULT
  
-- Answer q4c
select neighbourhood,
       property_type,
       room_type,
       accommodates,
       avg_estimated_revenue_per_active_listing
from   public.q4c
where  neighbourhood in (select neighbourhood
                         from   public.q4c1)
       and avg_estimated_revenue_per_active_listing = 484928.40;

      
--------------------------------------------------------------------------------------
-- QUESTION D
-- For hosts with multiple listings, are their properties concentrated within the same
-- LGA, or are they distributed across different LGAs?
--------------------------------------------------------------------------------------

-- List hosts with multiple listings
create or replace view public.q4d
as
  (select distinct host_id,
                   count(distinct listing_id)     as number_of_listings,
                   count(distinct listing_lga_id) as number_of_distinct_lgas
   from   gold.fact_price_review
   group  by host_id
   having count(distinct listing_id) >= 2);

-- 3909 hosts have properties in a single LGA
select count(distinct host_id)
from   public.q4d
where  number_of_distinct_lgas = 1;

-- 1186 hosts have properties across different LGAs
select count(distinct host_id)
from   public.q4d
where  number_of_distinct_lgas != 1;

--------------------------------------------------------------------------------------
-- QUESTION E
-- For hosts with a single Airbnb listing, does the estimated revenue over the last 12
-- months cover the annualised median mortgage repayment in the corresponding
-- LGA? Which LGA has the highest percentage of hosts that can cover it?
--------------------------------------------------------------------------------------

-- Base data
create or replace view public.q4e
as
  (select distinct fpr.host_id
                   as host_id,
                   fpr.listing_lga_id
                      as listing_lga_id,
                   dl.listing_id
                      as listing_id,
                   ( case
                       when dl.has_availability = 't' then 1.0
                       else 0.0
                     end ) * ( 30 - fpr.listing_availability_30 ) *
                   fpr.listing_price as
                   estimated_revenue_per_active_listing,
                   dc.median_mortgage_repay_monthly
                      as median_mortgage_repay_monthly
   from   gold.fact_price_review fpr
          left join gold.dim_census dc
                 on fpr.listing_lga_id = dc.lga_id
          left join gold.dim_listing dl
                 on fpr.listing_id = dl.listing_id
   where  fpr.host_lga_id is not null
          -- filter: hosts that haven't provided a neighbourhood
          and fpr.date between '2020-05-01' :: date and '2021-04-01' :: date
  -- filter: data within the year of current analysis
  );

-- Filter from base data to get the annualised revenue per active listing and annualised median mortgage repayment for
-- hosts with only 1 property
create or replace view public.q4e1
as
  (select distinct host_id,
                   listing_lga_id,
                   count(*)                                                  as
                      number_of_host_records,
                   ------------------------------------------------------------------------------------------------------------------
                   -- If the number of records is less than 12, the annualised figure is calculated by 
                   -- dividing the sum of all records by the number of records, then times 12 again to get the most accurate estimate.
                   -- In the case of all 12 available records, the annualised result is just the sum of all months. 
                   sum(estimated_revenue_per_active_listing) / count(*) * 12 as
                   annualised_revenue_per_active_listing,
                   sum(median_mortgage_repay_monthly) / count(*) * 12        as
                   annualised_median_mortgage_repay
   		     ------------------------------------------------------------------------------------------------------------------
   from   public.q4e
   group  by host_id,
             listing_lga_id
   having count(distinct listing_id) = 1);

create or replace view public.q4e2
as
  (select listing_lga_id,
          sum(case
                when annualised_revenue_per_active_listing
                     - annualised_median_mortgage_repay
                     >= 0 then 1.0
                else 0.0
              end)                            as
             count_hosts_that_can_cover_mortgage,
          sum(case
                when annualised_revenue_per_active_listing
                     - annualised_median_mortgage_repay
                     < 0 then 1.0
                else 0.0
              end)                            as
             count_hosts_that_cannot_cover_mortgage,
          round(sum(case
                      when annualised_revenue_per_active_listing
                           - annualised_median_mortgage_repay
                           >= 0 then 1.0
                      else 0.0
                    end) / count(*) * 100, 2) as
          percentage_hosts_that_can_cover_mortgage_in_lga
   from   public.q4e1
   group  by listing_lga_id);

select dl.lga_name,
       qe.count_hosts_that_can_cover_mortgage,
       qe.count_hosts_that_cannot_cover_mortgage,
       qe.percentage_hosts_that_can_cover_mortgage_in_lga
from   public.q4e2 qe
       left join gold.dim_lga dl
              on qe.listing_lga_id = dl.lga_id
order  by qe.percentage_hosts_that_can_cover_mortgage_in_lga desc;
