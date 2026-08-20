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


DATA_PATH = '/opt/airflow/data_download'

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
    catchup=False,
    max_active_runs=1,
) as dag:

    #curl https://raw.githubusercontent.com/fawazahmed0/currency-api/1/latest/currencies/eur.json -s > /opt/airflow/data_download/raw/euro_{{ ds }}.json
    # Создание папок и загрузка json данными
    check_data_folder = BashOperator(
        task_id='fetch_data_from_api',
        bash_command = 
        """
        mkdir -p /opt/airflow/data_download/raw
        mkdir -p /opt/airflow/data_download/clean
        curl https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/eur.json -s > /opt/airflow/data_download/raw/euro_{{ ds }}.json
        curl https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json -s > /opt/airflow/data_download/raw/usd_{{ ds }}.json
        """,
    )

    def process_data(**context):
        execution_data = context['ds']

        # EUR
        raw_file_path_eur = f'{DATA_PATH}/raw/euro_{execution_data}.json'
        clean_file_data = f'{DATA_PATH}/clean/euro_{execution_data}.parquet'
        

        #raw_file_path_dol = f'/data/raw/usd_{execution_data}.json'
        #clean_data_dol = f'/data/clean/usd_{execution_data}.parquet'

        if not os.path.exists(raw_file_path_eur):
            print(f"❌ Файл не найден: {raw_file_path_eur}")
            print(f"📂 Проверяем содержимое {DATA_PATH}/raw/:")
            if os.path.exists(f'{DATA_PATH}/raw/'):
                for f in os.listdir(f'{DATA_PATH}/raw/'):
                    print(f"  - {f}")
            else:
                print(f"❌ Папка {DATA_PATH}/raw/ не существует!")
            raise FileNotFoundError(f"Файл не найден: {raw_file_path_eur}")
        
        with open(raw_file_path_eur, 'r') as f:
            data_eur = json.load(f)

        rub_rate = data_eur['eur'].get('rub')
        date = data_eur['date']

        df = pd.DataFrame([{
            'currency': 'RUB',
            'rate': round(rub_rate,2),
            'start_currency': 'EUR',
            'date': date,
        }])

        df.to_parquet(clean_file_data,index=False)

        df_read = pd.read_parquet(f'{clean_file_data}')
        print(f"Полный путь: {clean_file_data}, \nзначение:\n{df_read}")



    transform_data = PythonOperator(
        task_id='Transform_data',
        python_callable=process_data,
    )



pg_hook = PostgresHook(postgres_conn_id='local_pg')

check_data_folder >> transform_data
