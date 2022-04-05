# citibike-DEZ
This Repo is a project for the Data Engineering Zoomcamp processing citibike dataset from zipped csv files through pipelines demonstrated in the course.

https://ride.citibikenyc.com/system-data

I use airflow to process the files from 2019-2020 into parquet files and big query on Google Cloud. Then, I use spark to do some spark sql from imported parquet files. Lastly, some charts of the data are made on Google Data Studio using the big query data source. 
