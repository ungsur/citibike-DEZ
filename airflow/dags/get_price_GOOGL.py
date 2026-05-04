from airflow import DAG 
from airflow.sdk import task
from datetime import datetime


with DAG(dag_id="get_price_GOOGL",
         start_date=datetime(2021,1,1),
         schedule="@daily",
         catchup=False) as dag:
    
         @task
         def extract(symbol):
             return symbol
         
         @task
         def process(symbol):
             return symbol
         
         @task
         def store(symbol):
             return symbol
         store(process(extract(126)))
         