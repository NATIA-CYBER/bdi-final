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

def start_task(**context):
    """Start the DAG execution."""
    print("Starting readsb-hist data processing...")
    return "Started"

def download_files(**context):
    """Download readsb-hist files for the first day of each month."""
    execution_date = context['execution_date']
    
    # Only process if it's the first day of the month
    if execution_date.day != 1:
        print(f"Skipping {execution_date} as it's not the first day of the month")
        return "Skipped"
    
    # Format date for URL
    date_str = execution_date.strftime('%Y/%m/%d')
    s3 = boto3.client('s3')
    
    # Check if file already exists (idempotency)
    raw_key = f'raw/readsb_hist/{date_str}/data.json'
    try:
        s3.head_object(Bucket=AWS_BUCKET, Key=raw_key)
        print(f"File {raw_key} already exists in S3, skipping...")
        return f"s3://{AWS_BUCKET}/{raw_key}"
    except:
        pass
    
    # Download data
    url = f"https://samples.adsbexchange.com/readsb-hist/{date_str}/readsb-hist.json"
    response = requests.get(url)
    data = response.json()
    
    # Store in S3
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=raw_key,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    
    return f"s3://{AWS_BUCKET}/{raw_key}"

def process_data(**context):
    """Process readsb-hist data."""
    execution_date = context['execution_date']
    
    # Only process if it's the first day of the month
    if execution_date.day != 1:
        print(f"Skipping {execution_date} as it's not the first day of the month")
        return "Skipped"
    
    date_str = execution_date.strftime('%Y/%m/%d')
    s3 = boto3.client('s3')
    
    # Get raw data
    raw_key = f'raw/readsb_hist/{date_str}/data.json'
    response = s3.get_object(Bucket=AWS_BUCKET, Key=raw_key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    
    # Process data
    processed_data = []
    for record in data:
        processed_record = {
            'timestamp': record.get('timestamp'),
            'icao': record.get('icao'),
            'callsign': record.get('callsign'),
            'latitude': record.get('lat'),
            'longitude': record.get('lon'),
            'altitude': record.get('alt_baro'),
            'ground_speed': record.get('gs'),
            'track': record.get('track'),
            'processed_at': datetime.now().isoformat()
        }
        processed_data.append(processed_record)
    
    # Store processed data
    prepared_key = f'prepared/readsb_hist/{date_str}/data.json'
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=prepared_key,
        Body=json.dumps(processed_data),
        ContentType='application/json'
    )
    
    return f"s3://{AWS_BUCKET}/{prepared_key}"

def end_task(**context):
    """End the DAG execution."""
    print("Finished readsb-hist data processing.")
    return "Finished"

with DAG(
    'readsb_history',
    default_args=default_args,
    description='Download and process readsb-hist data for first day of each month',
    schedule_interval='@daily',
    catchup=False,
    tags=['aircraft', 'history']
) as dag:

    start_task = PythonOperator(
        task_id='start',
        python_callable=start_task,
    )

    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_files,
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    end_task = PythonOperator(
        task_id='end',
        python_callable=end_task,
    )

    start_task >> download_task >> process_task >> end_task
