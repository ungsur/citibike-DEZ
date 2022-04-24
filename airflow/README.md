Docker compose in this directory creates a local instance of airflow.

Please populate the following parameters in your .env file:
COMPOSE_PROJECT_NAME=<A Project Name>
GOOGLE_APPLICATION_CREDENTIALS=<Google credentials json file>
GCP_PROJECT_ID=<Google Project ID>
GCP_GCS_BUCKET=<Location of GCS Bucket to store files>


Airflow has three dags:
data_ingestion_csv_to_gcs_dag takes files from the citibike data url and converts it to parquet files loaded onto Google Cloud Storage

data_ingestion_gcs_dag extracts the data from the csv files and converts them to parquet files in Google Cloud Storage. 

gcs_to_bq_dag loads the parquet files into Big Query into a table partitioned by date.
