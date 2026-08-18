import csv, json, os
from datetime import timedelta

import airflow
import pandas as pd
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#from airflow.providers.postgres.operators.postgres import PostgresOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

default_args = {
 'owner': 'alex',
 'retries': 2,
 'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id = "banking_analytics_pipeline",
    default_args = default_args,
    schedule_interval = '30 7 * * *', # min hour month * ( days week from 1-7 ) +8 UTC 
    description = 'Выгрузка и обработка банковских данных',
    start_date = days_ago(1) ,
) as dag:

    # Создание папок и загрузка json данными
    check_data_folder = BashOperator(
        task_id='fetch_data_from_api',
        bash_command = 
        """
        mkdir -p ~/data/raw
        mkdir -p ~/data/clean
        curl https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/eur.json -s > ~/data/raw/euro_{{ ds }}.json
        curl https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json -s > ~/data/raw/usd_{{ ds }}.json
        """,
    )

    def process_data(**context):
        execution_data = context['ds']
        home = os.path.expanduser("~")
        # EUR
        raw_file_path = f'{home}/data/raw/euro_{execution_data}.json'
        clean_file_data = f'{home}/data/clean/euro_{execution_data}.parquet'

        #raw_file_path_dol = f'/data/raw/usd_{execution_data}.json'
        #clean_data_dol = f'/data/clean/usd_{execution_data}.parquet'

        with open(raw_file_path,'r') as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        df['date'] = execution_data # время
        df.to_parquet(clean_file_data)


    transform_data = PythonOperator(
        task_id='Transform_data',
        python_callable=process_data,
    )



pg_hook = PostgresHook(postgres_conn_id='local_pg')

check_data_folder >> transform_data
