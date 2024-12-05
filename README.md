# ELT Data Pipeline with Apache Airflow and dbt

## Summary

This project aims to build a production-ready data pipeline for ingesting, transforming and
storing 12 months of Airbnb data using the **Extract-Load-Transform (ELT)** integration
framework. It also combines NSW census data to answer a range of business questions
related to hosts’ listing performance, demographic information at the local government
area (LGA) level, and financing health.

To this end, listings and census data are extracted from source systems, and the extraction process assumes a monthly schedule in order to implement **Type-2 Slowly Changing Dimensions (SCD-2)** as new data arrives.

## How to use this repository

This repository is structured into the following modules:
- `models`: where the data models live. This directory is further sub-structured into
`bronze`, `silver`, and `gold` layers, representing the Medallion architecture
- `snapshots`: stores the snapshot models that captures changes in dimensions.
This follows a **Type-2 Slowly Changing Dimensions** framework, where records
are marked with `valid_from` and `valid_to` timestamps
- `landing`: contains a SQL script to create the landing schema for all data. 
This script is executed before the main pipeline is executed, in order to 
create the baseline into which all data will land
- `macros`: contains a SQL script to change the default name of the *dbt* tables
into a more structured naming convention
- `dag`: stores the Directed Acyclic Graph script written in Python, using 
Airflow as the underlying engine. This script will orchestrate the whole 
data pipeline upon completion of the data models
- `report`: stores the final report of the project 

The tree structure of the repository is in the following:

```
.
├── README.md
├── analysis
│   └── adhoc_analysis.sql
├── dag
│   └── dag.py
├── dbt_project.yml
├── landing
│   └── landing.sql
├── macros
│   └── generate_schema_name.sql
├── models
│   ├── bronze
│   │   ├── b_g01_census_2016_nsw_lga.sql
│   │   ├── b_g02_census_2016_nsw_lga.sql
│   │   ├── b_listing.sql
│   │   ├── b_nsw_lga_code.sql
│   │   └── b_nsw_lga_suburb.sql
│   ├── gold
│   │   ├── mart
│   │   │   ├── g_dm_host_neighbourhood.sql
│   │   │   ├── g_dm_listing_neighbourhood.sql
│   │   │   └── g_dm_property_type.sql
│   │   └── star
│   │       ├── g_dim_census.sql
│   │       ├── g_dim_host.sql
│   │       ├── g_dim_lga.sql
│   │       ├── g_dim_listing.sql
│   │       ├── g_dim_property_type.sql
│   │       ├── g_dim_room_type.sql
│   │       ├── g_dim_suburb.sql
│   │       └── g_fact_price_review.sql
│   ├── silver
│   │   ├── s_dim_listing.sql
│   │   ├── s_fact_listing.sql
│   │   ├── s_g01_census_2016_nsw_lga.sql
│   │   ├── s_g02_census_2016_nsw_lga.sql
│   │   ├── s_nsw_lga_code.sql
│   │   └── s_nsw_lga_suburb.sql
│   └── sources.yml
├── package-lock.yml
├── packages.yml
├── report
│   └── ELT_Pipeline_Report.pdf
└── snapshots
    └── listing_snapshot.sql
```

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

## The ELT Pipeline

### Extract
The data is stored in a Google Cloud bucket, organised into `dimensions` and 
`facts` data subfolders, for neat organisation.

In each subfolder, place an `archive` directory to land data already ingested
by the pipeline. This avoids duplication when the pipeline is re-run.

### Load + Transform
The data is loaded onto a Google Cloud SQL instance. The landing schema follows
a medallion architecture:
- **Bronze schema**: stores raw data extracted from Google Cloud bucket, with 
minimal changes.
- **Silver schema**: stores data that are cleaned, renamed for consistency, and 
of the correct data types.
- **Gold schema**: sub-split into a `star` schema, and a `mart` schema:
    - **Star**: dimensions and facts
    - **Mart**: various aggregations for different business functions, 
    materialised as views

## License
This work is protected by the MIT License. See `LICENSE` for more information.
