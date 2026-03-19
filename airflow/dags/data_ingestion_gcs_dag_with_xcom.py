import os
import logging

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import storage
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import zipfile
from datetime import datetime


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
AIRFLOW_HOME_DATA =  os.environ.get("AIRFLOW_HOME_DATA", "/opt/airflow/data")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "citibike_data_all")

URL_PREFIX = "https://s3.amazonaws.com/tripdata/"
URL_TEMPLATE = (
    URL_PREFIX + "{{ logical_date.strftime('%Y%m') }}-citibike-tripdata.zip"
)
OUTPUT_ZIPFILE_TEMPLATE = (
    "{{ logical_date.strftime('%Y%m') }}-citibike-tripdata.zip"
)
OUTPUT_YEAR_TEMPLATE = "{{ logical_date.strftime('%Y') }}"


def process_zipfile(src_file, path_dir, ti):
    if not src_file.endswith(".zip"):
        logging.error("Can only accept source files in ZIP format, for the moment")
        return
    with zipfile.ZipFile(src_file) as z:
        zipfilelist = z.namelist()
        z.extractall(path=path_dir)   
    ti.xcom_push(key='zipfilelist', value=zipfilelist)


def format_to_parquet(csv_dir, pq_dir, ti):
    zipfilelist = ti.xcom_pull(task_ids='process_zipfile_task', key='zipfilelist')
   
    for file in zipfilelist:
        pq_filename = file.replace(".csv", ".parquet")
        csv_file = f"{csv_dir}/{file}"
        pq_file = f"{pq_dir}/{pq_filename}"
        convert_options = pv.ConvertOptions(
            column_types={  "ride_id": pa.string(),
                            "rideable_type": pa.string(),
                            "started_at": pa.timestamp('ms'),
                            "ended_at": pa.timestamp('ms'),
                            "start_station_name": pa.string(),
                            "start_station_id": pa.string(),
                            "end_station_name": pa.string(),
                            "end_station_id": pa.string(),
                            "start_lat": pa.float64(),
                            "start_lng": pa.float64(),
                            "end_lat": pa.float64(),
                            "end_lng": pa.float64(),
                            "member_casual": pa.string()}
                            )
        table = pv.read_csv(csv_file, convert_options=convert_options)
        pq.write_table(table, pq_file)


def upload_to_gcs(bucket, object_name, local_dir, filetype, ti):
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
    if filetype == "zip":    
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_dir)

    else:
        zipfilelist = ti.xcom_pull(task_ids='process_zipfile_task', key='zipfilelist')
    
        for file in zipfilelist:
            if filetype == "parquet":
                file_name = file.replace(".csv", ".parquet") 
            else:
                file_name = file
            blob_name = f"{object_name}/{file_name}"
            upload_file =f"{local_dir}/{file_name}"
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(upload_file)
            print(f"Uploaded {upload_file} to gs://{bucket}/{blob_name}") 
        
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "end_date": datetime(2024, 3, 1),
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_with_xcom_gcs_dag",
    default_args=default_args,
    catchup=True,
    schedule="0 6 2 * *",
    max_active_runs=1,
    tags=[PROJECT_ID],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        # bash_command='echo "{{ ds }}" "{{ logical_date.strftime(\'%Y%m\') }}"',
        bash_command=f"curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME}/{OUTPUT_ZIPFILE_TEMPLATE}",
    )

    process_zipfile_task = PythonOperator(
        task_id="process_zipfile_task",
        python_callable=process_zipfile,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME_DATA}/raw/{OUTPUT_YEAR_TEMPLATE}/{OUTPUT_ZIPFILE_TEMPLATE}",
            "path_dir": f"{AIRFLOW_HOME_DATA}/csv/{OUTPUT_YEAR_TEMPLATE}",
        },
    )   
    
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "csv_dir": f"{AIRFLOW_HOME_DATA}/csv/{OUTPUT_YEAR_TEMPLATE}",
            "pq_dir": f"{AIRFLOW_HOME_DATA}/pq/{OUTPUT_YEAR_TEMPLATE}",
        },
    )       

    local_zip_to_gcs_task = PythonOperator(
        task_id="local_zip_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_YEAR_TEMPLATE}/{OUTPUT_ZIPFILE_TEMPLATE}",
            "filetype": "zip",
            "local_dir": f"{AIRFLOW_HOME_DATA}/raw/{OUTPUT_YEAR_TEMPLATE}/{OUTPUT_ZIPFILE_TEMPLATE}",
        },
    )

    
    local_pq_to_gcs_task = PythonOperator(
        task_id="local_pq_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"pq/{OUTPUT_YEAR_TEMPLATE}",
            "filetype": "parquet",
            "local_dir": f"{AIRFLOW_HOME_DATA}/pq/{OUTPUT_YEAR_TEMPLATE}",
        },
    )

    local_csv_to_gcs_task = PythonOperator(
        task_id="local_csv_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"csv/{OUTPUT_YEAR_TEMPLATE}",
            "filetype": "csv",
            "local_dir": f"{AIRFLOW_HOME_DATA}/csv/{OUTPUT_YEAR_TEMPLATE}",
        },
    )
    
    (
        download_dataset_task
        >> process_zipfile_task 
        >> format_to_parquet_task
        >> [local_zip_to_gcs_task ,local_csv_to_gcs_task , local_pq_to_gcs_task]
    )
