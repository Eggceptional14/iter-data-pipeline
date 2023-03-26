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
from pipeline.utils.split_place_nested import split_place_nested
from pipeline.utils.place_data_transform import *


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
    schedule_interval='@weekly',
    dagrun_timeout=timedelta(minutes=300),
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
    task_location_cln = PythonOperator(
        task_id = 'location_cln',
        python_callable=location_cleaning
    )
    task_sha_cln = PythonOperator(
        task_id = 'sha_cln',
        python_callable=sha_cleaning
    )
    task_contact_cln = PythonOperator(
        task_id = 'contact_cln',
        python_callable=contact_cleaning
    )
    task_facilities_cln = PythonOperator(
        task_id = 'facilities_cln',
        python_callable=facilities_cleaning
    )
    task_services_cln = PythonOperator(
        task_id = 'services_cln',
        python_callable=services_cleaning
    )
    task_places_cln = PythonOperator(
        task_id = 'places_cln',
        python_callable=places_cleaning
    )
    task_places_split = PythonOperator(
        task_id = 'places_split',
        python_callable=split_place_nested
    )
    task_tag_cln = PythonOperator(
        task_id = 'tag_cln',
        python_callable=tag_data_cleaning
    )
    task_inf_cln = PythonOperator(
        task_id = 'inf_cln',
        python_callable=info_data_cleaning
    )
    task_mcl_cln = PythonOperator(
        task_id = 'mcl_cln',
        python_callable=michelin_data_cleaning
    )
    task_ophr_cln = PythonOperator(
        task_id = 'ophr_cln',
        python_callable=ophr_data_cleaning
    )
    task_room_cln = PythonOperator(
        task_id = 'room_cln',
        python_callable=room_data_cleaning
    )

task_get_raw_data >> task_get_detail >> task_split_nested
task_split_nested >> [task_location_cln, task_sha_cln, task_contact_cln, task_facilities_cln, task_services_cln]
task_split_nested >> task_places_cln