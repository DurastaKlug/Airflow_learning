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


DATA_PATH = '/opt/data_download'

default_args = {
 'owner': 'alex',
 'retries': 2,
 'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id = "banking_analytics_pipeline",
    default_args = default_args,
    schedule_interval = '30 7 * * *', # min hour month ( days week from 1-7 ) (+8 UTC по умл)
    description = 'Выгрузка и обработка банковских данных',
    start_date = days_ago(1) ,
) as dag:

    
    # Создание папок и загрузка json данными
    check_data_folder = BashOperator(
        task_id='fetch_data_from_api',
        bash_command = 
        f"""
        mkdir -p {DATA_PATH}/raw
        mkdir -p {DATA_PATH}/clean
        curl https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/eur.json -s > {DATA_PATH}/raw/euro_{{ ds }}.json
        curl https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json -s > {DATA_PATH}/raw/usd_{{ ds }}.json
        """,
    )

    def process_data(**context):
        execution_data = context['ds']

        # EUR
        raw_file_path = f'{DATA_PATH}/raw/euro_{execution_data}.json'
        clean_file_data = f'{DATA_PATH}/clean/euro_{execution_data}.parquet'

        #raw_file_path_dol = f'/data/raw/usd_{execution_data}.json'
        #clean_data_dol = f'/data/clean/usd_{execution_data}.parquet'

        if not os.path.exists(raw_file_path):
            print(f"❌ Файл не найден: {raw_file_path}")
            print(f"📂 Проверяем содержимое {DATA_PATH}/raw/:")
            if os.path.exists(f'{DATA_PATH}/raw/'):
                for f in os.listdir(f'{DATA_PATH}/raw/'):
                    print(f"  - {f}")
            else:
                print(f"❌ Папка {DATA_PATH}/raw/ не существует!")
            raise FileNotFoundError(f"Файл не найден: {raw_file_path}")
        
        with open(raw_file_path,'r') as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        df['date'] = execution_data # время
        df.to_parquet(clean_file_data)

        print(f"Полный путь: {clean_file_data}")



    transform_data = PythonOperator(
        task_id='Transform_data',
        python_callable=process_data,
    )



pg_hook = PostgresHook(postgres_conn_id='local_pg')

check_data_folder >> transform_data
