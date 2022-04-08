import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


import pyarrow.csv as pv
import pyarrow.parquet as pq
import zipfile
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# https://s3.amazonaws.com/tripdata/201501-citibike-tripdata.zip
dataset_file = "201501-citibike-tripdata.zip"
dataset_url = f"https://s3.amazonaws.com/tripdata/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
csvdataset_file = dataset_file.replace(".zip", ".csv")
parquet_file = dataset_file.replace(".zip", ".parquet")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "citibike_data_all")

URL_PREFIX = "https://s3.amazonaws.com/tripdata"
URL_TEMPLATE = (
    URL_PREFIX + "/{{ execution_date.strftime('%Y%m') }}-citibike-tripdata.csv.zip"
)
OUTPUT_ZIPFILE_TEMPLATE = (
    AIRFLOW_HOME + "/{{ execution_date.strftime('%Y%m') }}-citibike-tripdata.csv.zip"
)
OUTPUT_CSVFILE_TEMPLATE = OUTPUT_ZIPFILE_TEMPLATE.replace(".zip", "")


OUTPUT_PQFILE_TEMPLATE = (
    AIRFLOW_HOME + "/{{ execution_date.strftime('%Y%m') }}-citibike-tripdata.parquet"
)
OUTPUT_PQFILE_FILENAME = (
    "{{ execution_date.strftime('%Y%m') }}-citibike-tripdata.parquet"
)


def format_to_parquet(src_file, csv_file, pq_file):
    if not src_file.endswith(".zip"):
        logging.error("Can only accept source files in ZIP format, for the moment")
        return
    with zipfile.ZipFile(src_file) as z:
        z.extractall()
    table = pv.read_csv(csv_file)
    pq.write_table(table, pq_file)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
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
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2022, 1, 1),
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    default_args=default_args,
    catchup=True,
    schedule_interval="0 6 2 * *",
    max_active_runs=3,
    tags=["citibike-31437"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        # bash_command='echo "{{ ds }}" "{{ execution_date.strftime(\'%Y%m\') }}"',
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_ZIPFILE_TEMPLATE}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": OUTPUT_ZIPFILE_TEMPLATE,
            "csv_file": OUTPUT_CSVFILE_TEMPLATE,
            "pq_file": OUTPUT_PQFILE_TEMPLATE,
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_PQFILE_FILENAME}",
            "local_file": f"{OUTPUT_PQFILE_TEMPLATE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/"],
            },
        },
    )

    (
        download_dataset_task
        >> format_to_parquet_task
        >> local_to_gcs_task
        >> bigquery_external_table_task
    )
