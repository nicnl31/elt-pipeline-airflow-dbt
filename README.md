# ELT Data Pipeline with Apache Airflow and dbt

## Summary

This project aims to build a production-ready data pipeline for ingesting, transforming and
storing 12 months of Airbnb data using the **Extract-Load-Transform (ELT)** integration
framework. It also combines NSW census data to answer a range of business questions
related to hosts’ listing performance, demographic information at the local government
area (LGA) level, and financing health.

To this end, listings and census data are extracted from source systems, and the extraction process assumes a monthly schedule in order to implement **Type-2 Slowly Changing Dimensions (SCD-2)** as new data arrives.

## The data

### Airbnb data

Contains a year of listings information from *May
2020* to *April 2021*, for NSW only. These include:
- Listings: neighbourhood, property type/size, room type, active listing status
- Hosts: name, tenure, neighbourhood, and metrics (is superhost, 30-day availability,
number of reviews, review scores and ratings)

Data URL: https://insideairbnb.com/

### Census data
Contains the 2016 snapshot of NSW’s demographic information at the LGA level, including gender counts on:
- Population counts
- Population at different age groups
- Indigenous status: Aboriginal, Torres Strait Islander, or both
- Citizenship status: whether respondents are Australian citizens, and whether they were born in Australia or overseas
- Language: English or non-English speakers at home
- Education status by age group
- Private or other dwellings

There is also a referential dataset on LGA and suburb definitions, for joining information in the above datasets.

Data URL: https://www.abs.gov.au/census

