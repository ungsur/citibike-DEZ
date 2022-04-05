Docker compose in this directory creates a local instance of airflow.

Airflow has two dags:
data_ingestion_gcs_dag takes files from the citibike data url and converts it to parquet files loaded onto Google Cloud Storage

gcs_to_bq_dag loads the parquet files in the first dag into Big Query