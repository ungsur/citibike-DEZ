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

```
login:airflow
password: airflow.
```

run the DAGs from the airflow console.

Airflow has three dags:

1. data_ingestion_csv_to_gcs_dag - unzips raw files from the citibike data url to csv files loaded onto Google Cloud Storage

2. data_ingestion_gcs_dag - unzips and converts the raw files to parquet files in Google Cloud Storage.

The directory structure created has the following format:

```
<fileformat>/<YEAR>/<filename>
```

3. gcs_to_bq_dag - loads the parquet files into an external table on big query partitioned by date.
