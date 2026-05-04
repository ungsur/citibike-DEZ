import datetime
import pendulum
import os

import requests
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import storage
import zipfile


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
AIRFLOW_HOME_DATA =  os.environ.get("AIRFLOW_HOME_DATA", "/opt/airflow/data")


@dag(
    dag_id="download_weather",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
@task
download_dataset_task = BashOperator(
task_id="download_dataset_task",
# bash_command='echo "{{ ds }}" "{{ logical_date.strftime(\'%Y%m\') }}"',
bash_command=f"curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME_DATA}/raw/{OUTPUT_YEAR_TEMPLATE}/{OUTPUT_ZIPFILE_TEMPLATE}",
)