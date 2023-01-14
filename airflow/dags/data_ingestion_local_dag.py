import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq
import zipfile
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
AIRFLOW_HOME_DATA =  os.environ.get("AIRFLOW_HOME_DATA", "/opt/airflow/data/")

URL_PREFIX = "https://s3.amazonaws.com/tripdata/"
URL_TEMPLATE = (
    URL_PREFIX + "{{ execution_date.strftime('%Y%m') }}-citibike-tripdata.csv.zip"
)
OUTPUT_ZIPFILE_TEMPLATE = (
    "{{ execution_date.strftime('%Y%m') }}-citibike-tripdata.csv.zip"
)
OUTPUT_CSVFILE_TEMPLATE = OUTPUT_ZIPFILE_TEMPLATE.replace(".zip", "")
OUTPUT_YEAR_TEMPLATE = "{{ execution_date.strftime('%Y') }}"
OUTPUT_PQFILE_TEMPLATE = OUTPUT_ZIPFILE_TEMPLATE.replace(".csv.zip", ".parquet")

def format_to_csv(src_file, csv_file, path_dir):
    if not src_file.endswith(".zip"):
        logging.error("Can only accept source files in ZIP format, for the moment")
        return
    with zipfile.ZipFile(src_file) as z:
        z.extract(csv_file, path=path_dir)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2017, 1, 1),
    "end_date": datetime(2022, 4, 1),
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_local_dag",
    default_args=default_args,
    catchup=True,
    schedule_interval="0 6 2 * *",
    max_active_runs=4,
    tags=["citibike-31437"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME_DATA}/raw/{OUTPUT_ZIPFILE_TEMPLATE}",
    )

    local_zip_to_local_task = BashOperator(
        task_id="local_zip_to_local_task",
        # bash_command='echo "{{ ds }}" "{{ execution_date.strftime(\'%Y%m\') }}"',
        bash_command=f"mkdir -p {AIRFLOW_HOME_DATA}/raw/{OUTPUT_YEAR_TEMPLATE};\
                       mkdir -p {AIRFLOW_HOME_DATA}/csv/{OUTPUT_YEAR_TEMPLATE};\
                       mkdir -p {AIRFLOW_HOME_DATA}/pq/{OUTPUT_YEAR_TEMPLATE};\
                       mv {AIRFLOW_HOME_DATA}/raw/{OUTPUT_ZIPFILE_TEMPLATE} {AIRFLOW_HOME_DATA}/raw/{OUTPUT_YEAR_TEMPLATE}/{OUTPUT_ZIPFILE_TEMPLATE}",
    )

    unzip_to_csv_task = PythonOperator(
        task_id="format_to_csv_task",
        python_callable=format_to_csv,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME_DATA}/raw/{OUTPUT_YEAR_TEMPLATE}/{OUTPUT_ZIPFILE_TEMPLATE}",
            "csv_file": f"{OUTPUT_CSVFILE_TEMPLATE}",
            "path_dir": f"{AIRFLOW_HOME_DATA}/csv/{OUTPUT_YEAR_TEMPLATE}/",
        },
    )

    (
        download_dataset_task
        >> local_zip_to_local_task
        >> unzip_to_csv_task
    )
