# Базовые операторы
import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches", # name
    start_date=airflow.utils.dates.days_ago(14), # first start dag
    schedule_interval=None, # интервал с каким будет запускаться dag
)

download_launches = BashOperator( 
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L \
    'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)

def _get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=true,exists_ok=true) 

    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = (launch("image") for launch in launches["results"])

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f: # запись и чтение
                    f.write(response.content)
                print(f"Download {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectingError:
                 print(f"Could not connect {image_url}")

get_picture = PythonOperator(
    task_id="get_picture",
    python_callable=_get_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There is now images $(ls /tmp/images | wc -l) images."',
    dag=dag,
)

download_launches >> get_picture >>  notify
