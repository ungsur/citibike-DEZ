# Airflow README

Docker compose in this directory creates a local instance of airflow.

Populate the following parameters in the .env file located in the airflow directory:
COMPOSE_PROJECT_NAME=<A Project Name>
GOOGLE_APPLICATION_CREDENTIALS=<Google credentials json file location>
GCP_PROJECT_ID=<Google Project ID>
GCP_GCS_BUCKET=<Location of GCS Bucket to store files>

Run the following commands from the airflow directory to start airflow:

1. docker compose build
2. docker compose up airflow-init
3. docker compose up

After docker has brought all of the instances up, connect to the 
[airflow webserver](http://localhost:8080)

Login with login:airflow and password: airflow.
run the three dags.

Airflow has three dags:
data_ingestion_csv_to_gcs_dag takes raw files from the citibike data url and unzips to csv files loaded onto Google Cloud Storage

data_ingestion_gcs_dag extracts the csv files from the raw files and converts them to parquet files in Google Cloud Storage.

The directory structure on GCS is '<fileformat>/<YEAR>/<filename>'(eg. pq/2019/01901-citibike-tripdata.pq)

gcs_to_bq_dag loads the parquet files into Big Query into an external table partitioned by date.
