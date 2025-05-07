from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_fuel_data():
    url = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
    response = requests.get(url)
    data = response.json()
    
    # Store in S3
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='your-bucket-name',
        Key='raw/fuel_consumption/data.json',
        Body=json.dumps(data)
    )

with DAG(
    'aircraft_fuel_consumption',
    default_args=default_args,
    description='Download aircraft fuel consumption rates',
    schedule_interval='@daily',
    catchup=False
) as dag:

    download_task = PythonOperator(
        task_id='download_fuel_data',
        python_callable=download_fuel_data,
    )
