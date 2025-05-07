from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import gzip
import json
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_aircraft_db():
    url = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
    response = requests.get(url)
    
    # Decompress gzip data
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        data = json.loads(gz.read().decode('utf-8'))
    
    # Store in S3
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='your-bucket-name',
        Key='raw/aircraft_db/data.json',
        Body=json.dumps(data)
    )

with DAG(
    'aircraft_database',
    default_args=default_args,
    description='Download and process aircraft database',
    schedule_interval='@daily',
    catchup=False
) as dag:

    download_task = PythonOperator(
        task_id='download_aircraft_db',
        python_callable=download_aircraft_db,
    )
