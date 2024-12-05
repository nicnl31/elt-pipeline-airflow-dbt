import os
import logging
import requests
import time
import shutil

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2 import sql
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'bde-assignment-3',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='assignment-3-part-1',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"
DIMENSIONS = AIRFLOW_DATA + "/dimensions/"
FACTS = AIRFLOW_DATA + "/facts/"

#########################################################
#
#   Sleep function for waiting between DBT runs
#
#########################################################

def sleep_func(seconds, **kwargs):
    time.sleep(seconds)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################

def import_load_dim_2016Census_G01_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    category_file_path = DIMENSIONS + '2016Census_G01_NSW_LGA.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 2016Census_G01_NSW_LGA.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)

    if len(df) > 0:
        col_names = list(df.columns)
        values = df[col_names].to_dict('split')['data']
        logging.info(values)
        insert_sql = """
                    INSERT INTO bronze.raw_2016Census_G01_NSW_LGA(
                        LGA_CODE_2016, 
                        Tot_P_M, 
                        Tot_P_F,
                        Tot_P_P,
                        Age_0_4_yr_M,
                        Age_0_4_yr_F,
                        Age_0_4_yr_P,
                        Age_5_14_yr_M,
                        Age_5_14_yr_F,
                        Age_5_14_yr_P,
                        Age_15_19_yr_M,
                        Age_15_19_yr_F,
                        Age_15_19_yr_P,
                        Age_20_24_yr_M,
                        Age_20_24_yr_F,
                        Age_20_24_yr_P,
                        Age_25_34_yr_M,
                        Age_25_34_yr_F,
                        Age_25_34_yr_P,
                        Age_35_44_yr_M,
                        Age_35_44_yr_F,
                        Age_35_44_yr_P,
                        Age_45_54_yr_M,
                        Age_45_54_yr_F,
                        Age_45_54_yr_P,
                        Age_55_64_yr_M,
                        Age_55_64_yr_F,
                        Age_55_64_yr_P,
                        Age_65_74_yr_M,
                        Age_65_74_yr_F,
                        Age_65_74_yr_P,
                        Age_75_84_yr_M,
                        Age_75_84_yr_F,
                        Age_75_84_yr_P,
                        Age_85ov_M,
                        Age_85ov_F,
                        Age_85ov_P,
                        Counted_Census_Night_home_M,
                        Counted_Census_Night_home_F,
                        Counted_Census_Night_home_P,
                        Count_Census_Nt_Ewhere_Aust_M,
                        Count_Census_Nt_Ewhere_Aust_F,
                        Count_Census_Nt_Ewhere_Aust_P,
                        Indigenous_psns_Aboriginal_M,
                        Indigenous_psns_Aboriginal_F,
                        Indigenous_psns_Aboriginal_P,
                        Indig_psns_Torres_Strait_Is_M,
                        Indig_psns_Torres_Strait_Is_F,
                        Indig_psns_Torres_Strait_Is_P,
                        Indig_Bth_Abor_Torres_St_Is_M,
                        Indig_Bth_Abor_Torres_St_Is_F,
                        Indig_Bth_Abor_Torres_St_Is_P,
                        Indigenous_P_Tot_M,
                        Indigenous_P_Tot_F,
                        Indigenous_P_Tot_P,
                        Birthplace_Australia_M,
                        Birthplace_Australia_F,
                        Birthplace_Australia_P,
                        Birthplace_Elsewhere_M,
                        Birthplace_Elsewhere_F,
                        Birthplace_Elsewhere_P,
                        Lang_spoken_home_Eng_only_M,
                        Lang_spoken_home_Eng_only_F,
                        Lang_spoken_home_Eng_only_P,
                        Lang_spoken_home_Oth_Lang_M,
                        Lang_spoken_home_Oth_Lang_F,
                        Lang_spoken_home_Oth_Lang_P,
                        Australian_citizen_M,
                        Australian_citizen_F,
                        Australian_citizen_P,
                        Age_psns_att_educ_inst_0_4_M,
                        Age_psns_att_educ_inst_0_4_F,
                        Age_psns_att_educ_inst_0_4_P,
                        Age_psns_att_educ_inst_5_14_M,
                        Age_psns_att_educ_inst_5_14_F,
                        Age_psns_att_educ_inst_5_14_P,
                        Age_psns_att_edu_inst_15_19_M,
                        Age_psns_att_edu_inst_15_19_F,
                        Age_psns_att_edu_inst_15_19_P,
                        Age_psns_att_edu_inst_20_24_M,
                        Age_psns_att_edu_inst_20_24_F,
                        Age_psns_att_edu_inst_20_24_P,
                        Age_psns_att_edu_inst_25_ov_M,
                        Age_psns_att_edu_inst_25_ov_F,
                        Age_psns_att_edu_inst_25_ov_P,
                        High_yr_schl_comp_Yr_12_eq_M,
                        High_yr_schl_comp_Yr_12_eq_F,
                        High_yr_schl_comp_Yr_12_eq_P,
                        High_yr_schl_comp_Yr_11_eq_M,
                        High_yr_schl_comp_Yr_11_eq_F,
                        High_yr_schl_comp_Yr_11_eq_P,
                        High_yr_schl_comp_Yr_10_eq_M,
                        High_yr_schl_comp_Yr_10_eq_F,
                        High_yr_schl_comp_Yr_10_eq_P,
                        High_yr_schl_comp_Yr_9_eq_M,
                        High_yr_schl_comp_Yr_9_eq_F,
                        High_yr_schl_comp_Yr_9_eq_P,
                        High_yr_schl_comp_Yr_8_belw_M,
                        High_yr_schl_comp_Yr_8_belw_F,
                        High_yr_schl_comp_Yr_8_belw_P,
                        High_yr_schl_comp_D_n_g_sch_M,
                        High_yr_schl_comp_D_n_g_sch_F,
                        High_yr_schl_comp_D_n_g_sch_P,
                        Count_psns_occ_priv_dwgs_M,
                        Count_psns_occ_priv_dwgs_F,
                        Count_psns_occ_priv_dwgs_P,
                        Count_Persons_other_dwgs_M,
                        Count_Persons_other_dwgs_F,
                        Count_Persons_other_dwgs_P 
                    )
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DIMENSIONS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(category_file_path, os.path.join(archive_folder, '2016Census_G01_NSW_LGA.csv'))
    return None


def import_load_dim_2016Census_G02_func(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    # Check if the file exists
    category_file_path = DIMENSIONS + '2016Census_G02_NSW_LGA.csv'
    if not os.path.exists(category_file_path):
        logging.info("No 2016Census_G02_NSW_LGA.csv file found.")
        return None
    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)

    if len(df) > 0:
        col_names = list(df.columns)
        values = df[col_names].to_dict('split')['data']
        logging.info(values)
        insert_sql = """
                    INSERT INTO bronze.raw_2016Census_G02_NSW_LGA(
                        LGA_CODE_2016,
                        Median_age_persons,
                        Median_mortgage_repay_monthly,
                        Median_tot_prsnl_inc_weekly,
                        Median_rent_weekly,
                        Median_tot_fam_inc_weekly,
                        Average_num_psns_per_bedroom,
                        Median_tot_hhd_inc_weekly,
                        Average_household_size
                    )
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DIMENSIONS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(category_file_path, os.path.join(archive_folder, '2016Census_G02_NSW_LGA.csv'))
    return None


def import_load_dim_NSW_LGA_CODE_func(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    # Check if the file exists
    category_file_path = DIMENSIONS + 'NSW_LGA_CODE.csv'
    if not os.path.exists(category_file_path):
        logging.info("No NSW_LGA_CODE.csv file found.")
        return None
    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)

    if len(df) > 0:
        col_names = list(df.columns)
        values = df[col_names].to_dict('split')['data']
        logging.info(values)
        insert_sql = """
                    INSERT INTO bronze.raw_NSW_LGA_CODE(
                        LGA_CODE,
                        LGA_NAME
                    )
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DIMENSIONS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(category_file_path, os.path.join(archive_folder, 'NSW_LGA_CODE.csv'))
    return None


def import_load_dim_NSW_LGA_SUBURB_func(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    # Check if the file exists
    category_file_path = DIMENSIONS + 'NSW_LGA_SUBURB.csv'
    if not os.path.exists(category_file_path):
        logging.info("No NSW_LGA_SUBURB.csv file found.")
        return None
    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    df = df[["LGA_NAME", "SUBURB_NAME"]]

    if len(df) > 0:
        col_names = list(df.columns)
        values = df[col_names].to_dict('split')['data']
        logging.info(values)
        insert_sql = """
                    INSERT INTO bronze.raw_NSW_LGA_SUBURB(
                        LGA_NAME,
                        SUBURB_NAME
                    )
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DIMENSIONS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(category_file_path, os.path.join(archive_folder, 'NSW_LGA_SUBURB.csv'))
    return None

def import_load_facts_func(mo, yr, **kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    # Check if the file exists
    category_file_path = FACTS + f'{mo}_{yr}.csv'
    if not os.path.exists(category_file_path):
        logging.info(f"No {FACTS}{mo}_{yr}.csv file found.")
        return None
    # Generate dataframe by reading the CSV file
    df = pd.read_csv(category_file_path)
    fact_table_name = f"raw_{mo}_{yr}"
    if len(df) > 0:
        col_names = list(df.columns)
        values = df[col_names].to_dict('split')['data']
        logging.info(values)
        insert_sql = """
                    INSERT INTO bronze.raw_listing(
                        LISTING_ID,
                        SCRAPE_ID,
                        SCRAPED_DATE,
                        HOST_ID,
                        HOST_NAME,
                        HOST_SINCE,
                        HOST_IS_SUPERHOST,
                        HOST_NEIGHBOURHOOD,
                        LISTING_NEIGHBOURHOOD,
                        PROPERTY_TYPE,
                        ROOM_TYPE,
                        ACCOMMODATES,
                        PRICE,
                        HAS_AVAILABILITY,
                        AVAILABILITY_30,
                        NUMBER_OF_REVIEWS,
                        REVIEW_SCORES_RATING,
                        REVIEW_SCORES_ACCURACY,
                        REVIEW_SCORES_CLEANLINESS,
                        REVIEW_SCORES_CHECKIN,
                        REVIEW_SCORES_COMMUNICATION,
                        REVIEW_SCORES_VALUE
                    )
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(FACTS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(category_file_path, os.path.join(archive_folder, f'{mo}_{yr}.csv'))
    return None


#########################################################
#
#   Function to trigger dbt Cloud Job
#
#########################################################

def trigger_dbt_cloud_job(**kwargs):
    # Get the dbt Cloud URL, account ID, and job ID from Airflow Variables
    dbt_cloud_url = Variable.get("DBT_CLOUD_URL")
    dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
    dbt_cloud_job_id = Variable.get("DBT_CLOUD_JOB_ID")

    # Define the URL for the dbt Cloud job API dynamically using URL, account ID, and job ID
    url = f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}/jobs/{dbt_cloud_job_id}/run/"

    # Get the dbt Cloud API token from Airflow Variables
    dbt_cloud_token = Variable.get("DBT_CLOUD_API_TOKEN")

    # Define the headers and body for the request
    headers = {
        'Authorization': f'Token {dbt_cloud_token}',
        'Content-Type': 'application/json'
    }
    data = {
        "cause": "Triggered via API"
    }

    # Make the POST request to trigger the dbt Cloud job
    response = requests.post(url, headers=headers, json=data)

    # Check if the response is successful
    if response.status_code == 200:
        logging.info("Successfully triggered dbt Cloud job.")
        return response.json()
    else:
        logging.error(f"Failed to trigger dbt Cloud job: {response.status_code}, {response.text}")
        raise AirflowException("Failed to trigger dbt Cloud job.")


#########################################################
#
#   DAG Operator Setup
#
#########################################################

import_load_dim_2016Census_G01_task = PythonOperator(
    task_id="import_load_dim_2016Census_G01",
    python_callable=import_load_dim_2016Census_G01_func,
    provide_context=True,
    dag=dag
)

import_load_dim_2016Census_G02_task = PythonOperator(
    task_id="import_load_dim_2016Census_G02",
    python_callable=import_load_dim_2016Census_G02_func,
    provide_context=True,
    dag=dag
)

import_load_dim_NSW_LGA_CODE_task = PythonOperator(
    task_id="import_load_dim_NSW_LGA_CODE",
    python_callable=import_load_dim_NSW_LGA_CODE_func,
    provide_context=True,
    dag=dag
)

import_load_dim_NSW_LGA_SUBURB_task = PythonOperator(
    task_id="import_load_dim_NSW_LGA_SUBURB",
    python_callable=import_load_dim_NSW_LGA_SUBURB_func,
    provide_context=True,
    dag=dag
)

import_load_fact_05_2020_task = PythonOperator(
    task_id="import_load_facts_05_2020_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "05", "yr": "2020"},
    provide_context=True,
    dag=dag
)

import_load_fact_06_2020_task = PythonOperator(
    task_id="import_load_facts_06_2020_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "06", "yr": "2020"},
    provide_context=True,
    dag=dag
)

import_load_fact_07_2020_task = PythonOperator(
    task_id="import_load_facts_07_2020_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "07", "yr": "2020"},
    provide_context=True,
    dag=dag
)

import_load_fact_08_2020_task = PythonOperator(
    task_id="import_load_facts_08_2020_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "08", "yr": "2020"},
    provide_context=True,
    dag=dag
)

import_load_fact_09_2020_task = PythonOperator(
    task_id="import_load_facts_09_2020_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "09", "yr": "2020"},
    provide_context=True,
    dag=dag
)

import_load_fact_10_2020_task = PythonOperator(
    task_id="import_load_facts_10_2020_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "10", "yr": "2020"},
    provide_context=True,
    dag=dag
)

import_load_fact_11_2020_task = PythonOperator(
    task_id="import_load_facts_11_2020_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "11", "yr": "2020"},
    provide_context=True,
    dag=dag
)

import_load_fact_12_2020_task = PythonOperator(
    task_id="import_load_facts_12_2020_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "12", "yr": "2020"},
    provide_context=True,
    dag=dag
)

import_load_fact_01_2021_task = PythonOperator(
    task_id="import_load_facts_01_2021_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "01", "yr": "2021"},
    provide_context=True,
    dag=dag
)

import_load_fact_02_2021_task = PythonOperator(
    task_id="import_load_facts_02_2021_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "02", "yr": "2021"},
    provide_context=True,
    dag=dag
)

import_load_fact_03_2021_task = PythonOperator(
    task_id="import_load_facts_03_2021_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "03", "yr": "2021"},
    provide_context=True,
    dag=dag
)

import_load_fact_04_2021_task = PythonOperator(
    task_id="import_load_facts_04_2021_func",
    python_callable=import_load_facts_func,
    op_kwargs={"mo": "04", "yr": "2021"},
    provide_context=True,
    dag=dag
)

trigger_dbt_job_task_1 = PythonOperator(
    task_id='trigger_dbt_job_1',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)

trigger_dbt_job_task_2 = PythonOperator(
    task_id='trigger_dbt_job_2',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)

trigger_dbt_job_task_3 = PythonOperator(
    task_id='trigger_dbt_job_3',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)

trigger_dbt_job_task_4 = PythonOperator(
    task_id='trigger_dbt_job_4',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)

trigger_dbt_job_task_5 = PythonOperator(
    task_id='trigger_dbt_job_5',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)
trigger_dbt_job_task_6 = PythonOperator(
    task_id='trigger_dbt_job_6',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)
trigger_dbt_job_task_7 = PythonOperator(
    task_id='trigger_dbt_job_7',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)
trigger_dbt_job_task_8 = PythonOperator(
    task_id='trigger_dbt_job_8',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)
trigger_dbt_job_task_9 = PythonOperator(
    task_id='trigger_dbt_job_9',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)
trigger_dbt_job_task_10 = PythonOperator(
    task_id='trigger_dbt_job_10',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)
trigger_dbt_job_task_11 = PythonOperator(
    task_id='trigger_dbt_job_11',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)
trigger_dbt_job_task_12 = PythonOperator(
    task_id='trigger_dbt_job_12',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)

sleep_task_1 = PythonOperator(
    task_id='sleep_1',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)

sleep_task_2 = PythonOperator(
    task_id='sleep_2',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)

sleep_task_3 = PythonOperator(
    task_id='sleep_3',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)

sleep_task_4 = PythonOperator(
    task_id='sleep_4',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)

sleep_task_5 = PythonOperator(
    task_id='sleep_5',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)
sleep_task_6 = PythonOperator(
    task_id='sleep_6',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)
sleep_task_7 = PythonOperator(
    task_id='sleep_7',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)
sleep_task_8 = PythonOperator(
    task_id='sleep_8',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)
sleep_task_9 = PythonOperator(
    task_id='sleep_9',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)
sleep_task_10 = PythonOperator(
    task_id='sleep_10',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)
sleep_task_11 = PythonOperator(
    task_id='sleep_11',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)
sleep_task_12 = PythonOperator(
    task_id='sleep_12',
    python_callable=sleep_func,
    op_kwargs={"seconds": 130},
    provide_context=True,
    dag=dag
)

# Task Dependencies
[
    import_load_dim_2016Census_G01_task, 
    import_load_dim_2016Census_G02_task,
    import_load_dim_NSW_LGA_CODE_task,
    import_load_dim_NSW_LGA_SUBURB_task,
    import_load_fact_05_2020_task
] \
>> trigger_dbt_job_task_1 \
>> sleep_task_1 \
>> import_load_fact_06_2020_task \
>> trigger_dbt_job_task_2 \
>> sleep_task_2 \
>> import_load_fact_07_2020_task \
>> trigger_dbt_job_task_3 \
>> sleep_task_3 \
>> import_load_fact_08_2020_task \
>> trigger_dbt_job_task_4 \
>> sleep_task_4 \
>> import_load_fact_09_2020_task \
>> trigger_dbt_job_task_5 \
>> sleep_task_5 \
>> import_load_fact_10_2020_task \
>> trigger_dbt_job_task_6 \
>> sleep_task_6 \
>> import_load_fact_11_2020_task \
>> trigger_dbt_job_task_7 \
>> sleep_task_7 \
>> import_load_fact_12_2020_task \
>> trigger_dbt_job_task_8 \
>> sleep_task_8 \
>> import_load_fact_01_2021_task \
>> trigger_dbt_job_task_9 \
>> sleep_task_9 \
>> import_load_fact_02_2021_task \
>> trigger_dbt_job_task_10 \
>> sleep_task_10 \
>> import_load_fact_03_2021_task \
>> trigger_dbt_job_task_11 \
>> sleep_task_11 \
>> import_load_fact_04_2021_task \
>> trigger_dbt_job_task_12
