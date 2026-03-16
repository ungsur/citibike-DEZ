import os
import logging

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import storage

import zipfile
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
AIRFLOW_HOME_DATA =  os.environ.get("AIRFLOW_HOME_DATA", "/opt/airflow/data")

# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "citibike_data_all")

URL_PREFIX = "https://s3.amazonaws.com/tripdata/"
URL_TEMPLATE = (
    URL_PREFIX + "{{ logical_date.strftime('%Y%m') }}-citibike-tripdata.zip"
)
OUTPUT_ZIPFILE_TEMPLATE = (
    "{{ logical_date.strftime('%Y%m') }}-citibike-tripdata.zip"
)
OUTPUT_YEAR_TEMPLATE = "{{ logical_date.strftime('%Y') }}"

#def getziplist(ti):
#    zipfilelist = ti.xcom_pull(task_ids='process_zipfile_task', key='zipfilelist')
#    for file in zipfilelist:
#        print(f"File in zip: {file}")
    
    
def process_zipfile(src_file, ti):
    if not src_file.endswith(".zip"):
        logging.error("Can only accept source files in ZIP format, for the moment")
        return
    with zipfile.ZipFile(src_file) as z:
        zipfilelist = z.namelist()    
    ti.xcom_push(key='zipfilelist', value=zipfilelist)

def upload_to_gcs(bucket, object_name, local_file, ti):
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
    zipfilelist = ti.xcom_pull(task_ids='process_zipfile_task', key='zipfilelist')
   
    client = storage.Client()
    bucket = client.bucket(bucket)
    for file in zipfilelist:
        blob_name = f"{object_name}/{file}"
        upload_file =f"{local_file}/{file}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(upload_file)
        print(f"Uploaded {upload_file} to gs://{bucket}/{blob_name}")

    

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "end_date": datetime(2024, 4, 1),
    "retries": 1,
}

with DAG(
    dag_id="dag_with_zipfilelist",
    default_args=default_args,
    catchup=True,
    schedule="0 6 2 * *",
    max_active_runs=1,
    tags=[PROJECT_ID],
) as dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        # bash_command='echo "{{ ds }}" "{{ logical_date.strftime(\'%Y%m\') }}"',
        bash_command=f"curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME_DATA}/raw/{OUTPUT_YEAR_TEMPLATE}/{OUTPUT_ZIPFILE_TEMPLATE}",
    )

    process_zipfile_task = PythonOperator(
        task_id="process_zipfile_task",
        python_callable=process_zipfile,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME_DATA}/raw/{OUTPUT_YEAR_TEMPLATE}/{OUTPUT_ZIPFILE_TEMPLATE}",
        },
    )   
    
#   get_ziplist_task = PythonOperator(
#      task_id='get_ziplist_task',
#      python_callable=getziplist
#   )   

    local_csv_to_gcs_task = PythonOperator(
        task_id="local_csv_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"csv/{OUTPUT_YEAR_TEMPLATE}",
            "local_file": f"{AIRFLOW_HOME_DATA}/csv/{OUTPUT_YEAR_TEMPLATE}",
        },
    )
    
    (
        download_dataset_task 
     >> process_zipfile_task 
     >> local_csv_to_gcs_task
    ) 
     
    