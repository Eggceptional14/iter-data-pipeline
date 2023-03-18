import datetime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.utils.get_raw_data import get_raw_data
from pipeline.utils.get_detail import get_detail
from pipeline.utils.split_nested import split_nested
from pipeline.utils.remove_null import *
from pipeline.utils.data_cleaning import *
from pipeline.utils.split_category import split_category


default_args = {
    'owner': 'airflow',
    'start_date': datetime(year=2023, month=3, day=15),
    'email': ['62011213@kmitl.ac.th'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='iter_dag',
    default_args=default_args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=240),
) as dag:
    task_get_raw_data = PythonOperator(
        task_id = 'get_raw_data',
        python_callable=get_raw_data
    )
    task_get_detail = PythonOperator(
        task_id = 'get_detail',
        python_callable=get_detail
    )
    task_split_nested = PythonOperator(
        task_id='split_nested',
        python_callable=split_nested
    )
    task_split_category = PythonOperator(
        task_id = 'split_category',
        python_callable=split_category
    )
    task_location_std = PythonOperator(
        task_id = 'location_std',
        python_callable=location_standardize
    )
    # task_remove_null_acm = PythonOperator(
    #     task_id = 'remove_null_acm',
    #     python_callable=remove_null_accommodation
    # )
    # task_remove_null_atr = PythonOperator(
    #     task_id = 'remove_null_atr',
    #     python_callable=remove_null_attraction
    # )
    # task_remove_null_rst = PythonOperator(
    #     task_id = 'remove_null_rst',
    #     python_callable=remove_null_restaurant
    # )
    # task_remove_null_shp = PythonOperator(
    #     task_id = 'remove_null_shp',
    #     python_callable=remove_null_shop
    # )

task_get_raw_data >> task_get_detail >> task_split_nested >> task_location_std 
# task_split_category >> [task_remove_null_acm,
#                         task_remove_null_atr, 
#                         task_remove_null_rst, 
#                         task_remove_null_shp]
