{{ config(unique_key="lga_id", alias="dim_census") }}

select
    coalesce(g01.lga_id, g02.lga_id) as lga_id,
    -- -------------------------------------------------------------------------------------------
    -- CENSUS G01 DATA
    g01.total_population_m as total_population_m,
    g01.total_population_f as total_population_f,
    g01.total_population_p as total_population_p,
    g01.age_0_4_year_m as age_0_4_year_m,
    g01.age_0_4_year_f as age_0_4_year_f,
    g01.age_0_4_year_p as age_0_4_year_p,
    g01.age_5_14_year_m as age_5_14_year_m,
    g01.age_5_14_year_f as age_5_14_year_f,
    g01.age_5_14_year_p as age_5_14_year_p,
    g01.age_15_19_year_m as age_15_19_year_m,
    g01.age_15_19_year_f as age_15_19_year_f,
    g01.age_15_19_year_p as age_15_19_year_p,
    g01.age_20_24_year_m as age_20_24_year_m,
    g01.age_20_24_year_f as age_20_24_year_f,
    g01.age_20_24_year_p as age_20_24_year_p,
    g01.age_25_34_year_m as age_25_34_year_m,
    g01.age_25_34_year_f as age_25_34_year_f,
    g01.age_25_34_year_p as age_25_34_year_p,
    g01.age_35_44_year_m as age_35_44_year_m,
    g01.age_35_44_year_f as age_35_44_year_f,
    g01.age_35_44_year_p as age_35_44_year_p,
    g01.age_45_54_year_m as age_45_54_year_m,
    g01.age_45_54_year_f as age_45_54_year_f,
    g01.age_45_54_year_p as age_45_54_year_p,
    g01.age_55_64_year_m as age_55_64_year_m,
    g01.age_55_64_year_f as age_55_64_year_f,
    g01.age_55_64_year_p as age_55_64_year_p,
    g01.age_65_74_year_m as age_65_74_year_m,
    g01.age_65_74_year_f as age_65_74_year_f,
    g01.age_65_74_year_p as age_65_74_year_p,
    g01.age_75_84_year_m as age_75_84_year_m,
    g01.age_75_84_year_f as age_75_84_year_f,
    g01.age_75_84_year_p as age_75_84_year_p,
    g01.age_85_over_m as age_85_over_m,
    g01.age_85_over_f as age_85_over_f,
    g01.age_85_over_p as age_85_over_p,
    g01.count_census_night_home_m as count_census_night_home_m,
    g01.count_census_night_home_f as count_census_night_home_f,
    g01.count_census_night_home_p as count_census_night_home_p,
    g01.count_census_night_elsewhere_aus_m as count_census_night_elsewhere_aus_m,
    g01.count_census_night_elsewhere_aus_f as count_census_night_elsewhere_aus_f,
    g01.count_census_night_elsewhere_aus_p as count_census_night_elsewhere_aus_p,
    g01.indigenous_aboriginal_m as indigenous_aboriginal_m,
    g01.indigenous_aboriginal_f as indigenous_aboriginal_f,
    g01.indigenous_aboriginal_p as indigenous_aboriginal_p,
    g01.indigenous_torres_strait_m as indigenous_torres_strait_m,
    g01.indigenous_torres_strait_f as indigenous_torres_strait_f,
    g01.indigenous_torres_strait_p as indigenous_torres_strait_p,
    g01.indigenous_both_aboriginal_torres_strait_m
    as indigenous_both_aboriginal_torres_strait_m,
    g01.indigenous_both_aboriginal_torres_strait_f
    as indigenous_both_aboriginal_torres_strait_f,
    g01.indigenous_both_aboriginal_torres_strait_p
    as indigenous_both_aboriginal_torres_strait_p,
    g01.indigenous_total_m as indigenous_total_m,
    g01.indigenous_total_f as indigenous_total_f,
    g01.indigenous_total_p as indigenous_total_p,
    g01.birthplace_australia_m as birthplace_australia_m,
    g01.birthplace_australia_f as birthplace_australia_f,
    g01.birthplace_australia_p as birthplace_australia_p,
    g01.birthplace_elsewhere_m as birthplace_elsewhere_m,
    g01.birthplace_elsewhere_f as birthplace_elsewhere_f,
    g01.birthplace_elsewhere_p as birthplace_elsewhere_p,
    g01.language_spoken_home_english_only_m as language_spoken_home_english_only_m,
    g01.language_spoken_home_english_only_f as language_spoken_home_english_only_f,
    g01.language_spoken_home_english_only_p as language_spoken_home_english_only_p,
    g01.language_spoken_home_other_m as language_spoken_home_other_m,
    g01.language_spoken_home_other_f as language_spoken_home_other_f,
    g01.language_spoken_home_other_p as language_spoken_home_other_p,
    g01.australian_citizen_m as australian_citizen_m,
    g01.australian_citizen_f as australian_citizen_f,
    g01.australian_citizen_p as australian_citizen_p,
    g01.age_attend_education_institution_0_4_m
    as age_attend_education_institution_0_4_m,
    g01.age_attend_education_institution_0_4_f
    as age_attend_education_institution_0_4_f,
    g01.age_attend_education_institution_0_4_p
    as age_attend_education_institution_0_4_p,
    g01.age_attend_education_institution_5_14_m
    as age_attend_education_institution_5_14_m,
    g01.age_attend_education_institution_5_14_f
    as age_attend_education_institution_5_14_f,
    g01.age_attend_education_institution_5_14_p
    as age_attend_education_institution_5_14_p,
    g01.age_attend_education_institution_15_19_m
    as age_attend_education_institution_15_19_m,
    g01.age_attend_education_institution_15_19_f
    as age_attend_education_institution_15_19_f,
    g01.age_attend_education_institution_15_19_p
    as age_attend_education_institution_15_19_p,
    g01.age_attend_education_institution_20_24_m
    as age_attend_education_institution_20_24_m,
    g01.age_attend_education_institution_20_24_f
    as age_attend_education_institution_20_24_f,
    g01.age_attend_education_institution_20_24_p
    as age_attend_education_institution_20_24_p,
    g01.age_attend_education_institution_25_over_m
    as age_attend_education_institution_25_over_m,
    g01.age_attend_education_institution_25_over_f
    as age_attend_education_institution_25_over_f,
    g01.age_attend_education_institution_25_over_p
    as age_attend_education_institution_25_over_p,
    g01.school_year_12_completed_m as school_year_12_completed_m,
    g01.school_year_12_completed_f as school_year_12_completed_f,
    g01.school_year_12_completed_p as school_year_12_completed_p,
    g01.school_year_11_completed_m as school_year_11_completed_m,
    g01.school_year_11_completed_f as school_year_11_completed_f,
    g01.school_year_11_completed_p as school_year_11_completed_p,
    g01.school_year_10_completed_m as school_year_10_completed_m,
    g01.school_year_10_completed_f as school_year_10_completed_f,
    g01.school_year_10_completed_p as school_year_10_completed_p,
    g01.school_year_9_completed_m as school_year_9_completed_m,
    g01.school_year_9_completed_f as school_year_9_completed_f,
    g01.school_year_9_completed_p as school_year_9_completed_p,
    g01.school_year_8_below_completed_m as school_year_8_below_completed_m,
    g01.school_year_8_below_completed_f as school_year_8_below_completed_f,
    g01.school_year_8_below_completed_p as school_year_8_below_completed_p,
    g01.school_did_not_attend_school_m as school_did_not_attend_school_m,
    g01.school_did_not_attend_school_f as school_did_not_attend_school_f,
    g01.school_did_not_attend_school_p as school_did_not_attend_school_p,
    g01.count_private_dwellings_m as count_private_dwellings_m,
    g01.count_private_dwellings_f as count_private_dwellings_f,
    g01.count_private_dwellings_p as count_private_dwellings_p,
    g01.count_other_dwellings_m as count_other_dwellings_m,
    g01.count_other_dwellings_f as count_other_dwellings_f,
    g01.count_other_dwellings_p as count_other_dwellings_p,
    -- END CENSUS G01 DATA
    -- -------------------------------------------------------------------------------------------
    -- CENSUS G02 DATA
    g02.median_age as median_age,
    g02.median_mortgage_repay_monthly as median_mortgage_repay_monthly,
    g02.median_personal_income_weekly as median_personal_income_weekly,
    g02.median_rent_weekly as median_rent_weekly,
    g02.median_family_income_weekly as median_family_income_weekly,
    g02.average_num_persons_per_bedroom as average_num_persons_per_bedroom,
    g02.median_household_income_weekly as median_household_income_weekly,
    g02.average_household_size as average_household_size
-- END CENSUS G02 DATA
-- -------------------------------------------------------------------------------------------
from {{ ref("s_g01_census_2016_nsw_lga") }} g01
full join {{ ref("s_g02_census_2016_nsw_lga") }} g02 
on g01.lga_id = g02.lga_id
where g01.lga_id is not null and g02.lga_id is not null
