from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from food_inspections_etl import run_food_inspections_etl

defaults_args = {
    "owner":'Lp',
    'depends_on_past':False,
    'start_date':datetime(2024,7,15),
    'email':['lakshmipriya.diwakar@gmail.com'],
    'email_on_dailure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

dag=DAG(
    'food_inspections_dag',
    default_args=defaults_args,
    description='ETL code for Food Inspections Data'
)

run_etl = PythonOperator(
    task_id="complete_fi_etl",
    python_callable=run_food_inspections_etl,
    dag=dag
)

run_etl