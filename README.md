# citibike-DEZ

This Repo is a project for the Data Engineering Zoomcamp processing citibike dataset from zipped csv files through pipelines demonstrated in the course.

## Problem Statement

The dataset chosen for this project is the NYC Citibike dataset.  The Goal of the project is to perform ETL on the citibike csv files in order to create a dashboard from the data.

<https://ride.citibikenyc.com/system-data>

## Pipeline

I use terraform to create the project, data lake storage, and Big Query Dataset on Google cloud. Airflow with Docker is used to process the raw csv zipped files from 2017-2020 into parquet files on Google Cloud. I transform the data with DBT into Big Query fact and dimensional tables and create some charts of the data on Google Data Studio.

## Dashboard

The Dashboard includes a graph of Citibike data over from 2017-2021:
The report includes:
A graph of trip count over per month
A plot of Gender(Male/Female/Unknown) for all trips in the dataset
A Bubblemap geo chart of Station locations with the number of trips taken from each citibike location for a small area of midtown Manhattan.

![alt text](https://github.com/ungsur/citibike-DEZ/blob/main/CitibikeDataset.png?raw=true)

All charts used the tables created by DBT in Big query for the data sources.

The Geo chart was created with the help of this tutorial:
[Building geomaps in datastudio](https://michaelhoweely.com/2020/05/04/how-to-build-a-custom-google-map-in-data-studio-using-google-sheets-and-geocode)


[DataEngineering Zoomcamp completion certificate](https://github.com/ungsur/citibike-DEZ/blob/main/DataEngineeringCertificate.png?raw=true)


## Instructions to reproduce this pipeline

## Pre-requisites

1. Setup a Google Cloud account: <https://console.cloud.google.com/>

    Create an account with your Google email ID
    Setup a project and note down the "Project ID"
    Setup service account & authentication for this project
    Grant Viewer role to begin with.
    Download service-account-keys (.json) for auth.
    [Install Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)
    Set environment variable to point to your downloaded GCP keys:
    export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
    run 'gcloud auth application-default login'

    Create a Service account for terraform:

        IAM Roles for Service account:

        Go to the IAM section of IAM & Admin <https://console.cloud.google.com/iam-admin/iam>
        Click the Edit principal icon for your service account.
        Add these roles in addition to Viewer : Storage Admin + Storage Object Admin + BigQuery Admin
        Enable these APIs for your project:

        <https://console.cloud.google.com/apis/library/iam.googleapis.com>
        <https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com>
        Please ensure GOOGLE_APPLICATION_CREDENTIALS env-var is set.

        export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"#

2. Setup Docker with Docker Compose
For your platform, install Docker Desktop and Docker Compose from the [Docker Getting Started link](https://www.docker.com/get-started/)

3. [Install Terraform client](https://www.terraform.io/downloads)

4. [Install DBT link](https://docs.getdbt.com/dbt-cli/install/overview)
   [Install DBT BigQuery plugin](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)
    pip install dbt-bigquery


### After prerequisites are installed

Follow the README instructions in the following order:

1. [Terraform README](https://github.com/ungsur/citibike-DEZ/blob/main/terraform/README.md)

2. [Airflow README](https://github.com/ungsur/citibike-DEZ/blob/main/airflow/README.md)

3. [dbt README](https://github.com/ungsur/citibike-DEZ/blob/main/dbt/citibikedbt/README.md)

4. The dashboard in Google Data Studio was manually constructed and the report can be found [here](https://github.com/ungsur/citibike-DEZ/blob/main/Citibike_Dashboard.pdf)
