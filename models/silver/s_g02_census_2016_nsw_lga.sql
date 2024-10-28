{{ config(unique_key="lga_id", alias="g02_census_2016_nsw_lga") }}

select
    cast(substring(lga_code_2016, 4, 5) as int) as lga_id,
    median_age_persons as median_age,
    median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly as median_personal_income_weekly,
    median_rent_weekly,
    median_tot_fam_inc_weekly as median_family_income_weekly,
    average_num_psns_per_bedroom as average_num_persons_per_bedroom,
    median_tot_hhd_inc_weekly as median_household_income_weekly,
    average_household_size
from {{ ref("b_g02_census_2016_nsw_lga") }}
