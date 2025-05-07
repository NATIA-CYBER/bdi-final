from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AWS_BUCKET = os.getenv('AWS_BUCKET')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_fuel_data(**context):
    """Download aircraft fuel consumption rates."""
    url = "https://raw.githubusercontent.com/junzis/aircraft-emissions/master/data/fuel_consumption.csv"
    response = requests.get(url)
    data = response.text
    
    # Store in S3
    s3 = boto3.client('s3')
    timestamp = context['execution_date'].strftime('%Y/%m/%d')
    
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=f'raw/fuel_consumption/{timestamp}/data.csv',
        Body=data,
        ContentType='text/csv'
    )
    
    return f's3://{AWS_BUCKET}/raw/fuel_consumption/{timestamp}/data.csv'

def process_fuel_data(**context):
    """Process fuel consumption data."""
    s3 = boto3.client('s3')
    timestamp = context['execution_date'].strftime('%Y/%m/%d')
    raw_key = f'raw/fuel_consumption/{timestamp}/data.csv'
    
    # Get raw data
    response = s3.get_object(Bucket=AWS_BUCKET, Key=raw_key)
    data = response['Body'].read().decode('utf-8')
    
    # Process data (simple example - convert to JSON)
    lines = data.strip().split('\n')
    headers = lines[0].split(',')
    processed_data = []
    
    for line in lines[1:]:
        values = line.split(',')
        if len(values) == len(headers):
            record = dict(zip(headers, values))
            record['processed_at'] = datetime.now().isoformat()
            processed_data.append(record)
    
    # Store processed data
    prepared_key = f'prepared/fuel_consumption/{timestamp}/data.json'
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=prepared_key,
        Body=json.dumps(processed_data),
        ContentType='application/json'
    )
    
    return f's3://{AWS_BUCKET}/{prepared_key}'

with DAG(
    'fuel_consumption',
    default_args=default_args,
    description='Download and process aircraft fuel consumption rates',
    schedule_interval='@daily',
    catchup=False,
    tags=['aircraft', 'fuel']
) as dag:

    download_task = PythonOperator(
        task_id='download_fuel_data',
        python_callable=download_fuel_data,
    )

    process_task = PythonOperator(
        task_id='process_fuel_data',
        python_callable=process_fuel_data,
    )

    download_task >> process_task
