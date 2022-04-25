# DBT README

This DBT structure has a staging model and a core model. 
The staging model creates a View from the partitioned table created from the airflow DAG.

The Core model creates a fact table and several dimensional tables from the staging view.

Construct a profiles.yml file in ~/.dbt/profiles.yml with the following fields:

<dataset name:
  outputs:
    dev:
      dataset: <dataset name>
      fixed_retries: 1
      keyfile: <Google credentials file location>
      location: <google cloud region> 
      method: service-account
      priority: interactive
      project: <project-id>
      threads: 4
      timeout_seconds: 300
      type: bigquery
  target: dev

Run the following commands from the dbt/citibikedbt directory:

1. dbt build --var 'is_test_run: false'

2. dbt run --var 'is_test_run: false'
