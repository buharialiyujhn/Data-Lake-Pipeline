import os
from datetime import datetime, timedelta, date

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.combine_data import combine_data
from lib.fetch_movies_data import fetch_data_from_imdb
from lib.fetch_weather_data import fetch_data_from_weather
from lib.raw_to_fmt_imdb import convert_raw_to_formatted_imdb

from lib.index_elastic_search import index_data_to_elasticsearch
from lib.raw_to_fmt_weather import convert_raw_to_formatted_weather

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


with DAG(
       'my_first_dag',
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(minutes=5),
       },
       description='A first DAG',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:
   dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """

raw_imdb = PythonOperator(
    task_id='fetch_data_from_imdb',
    python_callable=fetch_data_from_imdb,
    provide_context=True,
    op_kwargs={'task_number': 'task1'},
    dag=dag
)
format_imdb = PythonOperator(
    task_id='convert_raw_to_formatted_imdb',
    python_callable=convert_raw_to_formatted_imdb,
    provide_context=True,
    op_kwargs={'task_number': 'task2'},
    dag=dag
)
raw_weather = PythonOperator(
    task_id='fetch_data_from_weather',
    python_callable=fetch_data_from_weather,
    provide_context=True,
    op_kwargs={'task_number': 'task3'},
    dag=dag
)
format_weather = PythonOperator(
    task_id='convert_raw_to_formatted_weather',
    python_callable=convert_raw_to_formatted_weather,
    provide_context=True,
    op_kwargs={'task_number': 'task4'},
    dag=dag
)
produce_usage = PythonOperator(
    task_id='produce_usage',
    python_callable=combine_data,
    provide_context=True,
    op_kwargs={'task_number': 'task5'},
    dag=dag
)
index_data_to_elasticsearch = PythonOperator(
    task_id='index_data_to_elasticsearch',
    python_callable=index_data_to_elasticsearch,
    provide_context=True,
    op_kwargs={'task_number': 'task6'},
    dag=dag
)

raw_imdb>>format_imdb
raw_weather>>format_weather
format_imdb>>produce_usage
format_weather>>produce_usage
produce_usage>>index_data_to_elasticsearch
