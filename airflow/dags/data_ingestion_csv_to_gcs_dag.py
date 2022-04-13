import os
import logging

from airflow import DAG
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
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "citibike_data_all")

URL_PREFIX = "https://s3.amazonaws.com/tripdata/"
URL_TEMPLATE = (
    URL_PREFIX + "{{ execution_date.strftime('%Y%m') }}-citibike-tripdata.csv.zip"
)
OUTPUT_ZIPFILE_TEMPLATE = (
    "{{ execution_date.strftime('%Y%m') }}-citibike-tripdata.csv.zip"
)
OUTPUT_CSVFILE_TEMPLATE = OUTPUT_ZIPFILE_TEMPLATE.replace(".zip", "")
OUTPUT_YEAR_TEMPLATE = "{{ execution_date.strftime('%Y') }}"


def format_to_csv(src_file, csv_file):
    if not src_file.endswith(".zip"):
        logging.error("Can only accept source files in ZIP format, for the moment")
        return
    with zipfile.ZipFile(src_file) as z:
        z.extractall()


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2017, 1, 1),
    "end_date": datetime(2021, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="data_zip_to_gcs_dag",
    default_args=default_args,
    catchup=True,
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    tags=["citibike-31437"],
) as dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        # bash_command='echo "{{ ds }}" "{{ execution_date.strftime(\'%Y%m\') }}"',
        bash_command=f"curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME}/{OUTPUT_ZIPFILE_TEMPLATE}",
    )

    unzip_to_csv_task = PythonOperator(
        task_id="format_to_csv_task",
        python_callable=format_to_csv,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{OUTPUT_ZIPFILE_TEMPLATE}",
            "csv_file": f"{AIRFLOW_HOME}/{OUTPUT_CSVFILE_TEMPLATE}",
        },
    )

    local_csv_to_gcs_task = PythonOperator(
        task_id="local_csv_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"csv/{OUTPUT_YEAR_TEMPLATE}/{OUTPUT_CSVFILE_TEMPLATE}",
            "local_file": f"{AIRFLOW_HOME}/{OUTPUT_CSVFILE_TEMPLATE}",
        },
    )

    (download_dataset_task >> unzip_to_csv_task >> local_csv_to_gcs_task)
