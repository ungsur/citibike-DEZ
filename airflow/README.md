Docker compose in this directory creates a local instance of airflow.

Airflow has two dags:
data_ingestion_gcs_dag takes files from the citibike data url and converts it to parquet files loaded onto Google Cloud Storage

data_ingestion_gcs_dag extracts the data from the csv files and converts them to parquet files in Google Cloud Storage. 

gcs_to_bq_dag loads the parquet files into Big Query into a table partitioned by date.
 