# citibike-DEZ

This Repo is a project for the Data Engineering Zoomcamp processing citibike dataset from zipped csv files through pipelines demonstrated in the course.

## Problem Statement:
The dataset chosen for this project is the NYC Citibike dataset.  The Goal of the project is to perform ETL on the citibike csv files in order to create a dashboard from the data. 

https://ride.citibikenyc.com/system-data

## Pipeline:

I use terraform to create the project, data lake storage, and Big Query Dataset on Google cloud. Airflow with Docker is used to process the raw csv zipped files from 2017-2020 into parquet files on Google Cloud. I transform the data with DBT into Big Query fact and dimensional tables and create some charts of the data on Google Data Studio.

## Dashboard:
The Dashboard includes a graph of Citibike data over from 2017-2021:
The report includes:
A graph of trip count over per month
A plot of Gender(Male/Female/Unknown) for all trips in the dataset
A Bubblemap geo chart of Station locations with the number of trips taken from each citibike location for a small area of midtown Manhattan.

All charts used the tables created by DBT in Big query for the data sources.

The Geo chart was created with the help of this tutorial:

https://michaelhoweely.com/2020/05/04/how-to-build-a-custom-google-map-in-data-studio-using-google-sheets-and-geocode/#:~:text=To%20do%20this%2C%20click%20to,used%20to%20make%20a%20report