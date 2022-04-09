import os

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "citibike_data_all")
DATASET = "citibike"
COL = "starttime"
CLUSTERCOL = "bikeid"

OUTPUT_PQ_FILENAME = "{{ execution_date.strftime('%Y%m') }}-citibike-tripdata.parquet"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_to_bq_ext_task",
    default_args=default_args,
    catchup=True,
    schedule_interval="0 6 2 * *",
    max_active_runs=3,
    tags=["citibike-31437"],
) as dag:
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
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
                    f"gs://{BUCKET}/pq/2017/*",
                    f"gs://{BUCKET}/pq/2018/*",
                    f"gs://{BUCKET}/pq/2019/*",
                    f"gs://{BUCKET}/pq/2020/*",
                ],
            },
        },
    )

    CREATE_BQ_TBL_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET}_table_partitioned \
        PARTITION BY DATE({COL}) \
        CLUSTER BY {CLUSTERCOL} AS \
        SELECT * FROM {BIGQUERY_DATASET}.{DATASET}_external_table;"

    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_{DATASET}_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        },
    )

bigquery_external_table_task >> bq_create_partitioned_table_job
