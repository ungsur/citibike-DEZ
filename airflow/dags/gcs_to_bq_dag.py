import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator ,
    BigQueryInsertJobOperator,
)


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "citibike_data_all")
DATASET = "citibike"
PARTITION_COL = "started_at"
CLUSTER_COL = "ride_id"

OUTPUT_PQ_FILENAME = "{{ logical_date.strftime('%Y%m') }}-citibike-tripdata.parquet"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.today('UTC').add(days=-1),
    "retries": 1,
}

with DAG(
    dag_id="gcs_to_bq_ext_task",
    default_args=default_args,
    catchup=True,
    schedule="0 6 2 * *",
    max_active_runs=3,
    tags=[PROJECT_ID],
) as dag:
    
    bigquery_create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="bigquery_create_dataset_task",
        dataset_id=BIGQUERY_DATASET,
        project_id=PROJECT_ID,
        location="US",
    )
    
    bigquery_external_table_task = BigQueryCreateTableOperator(
        task_id="bigquery_external_table_task",
        dataset_id=BIGQUERY_DATASET,
        table_id="citibike_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "citibike_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": [
                    f"gs://{BUCKET}/pq/2024/*",
                    f"gs://{BUCKET}/pq/2025/*",
                ],
            },
        },
    )

(
bigquery_create_dataset_task >> bigquery_external_table_task
)