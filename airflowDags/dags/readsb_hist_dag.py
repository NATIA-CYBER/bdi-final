from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import json
import os
from dotenv import load_dotenv
import pandas as pd
from typing import List
import logging

# Load environment variables
load_dotenv()

AWS_BUCKET = os.getenv('AWS_BUCKET')
MAX_FILES = 100  # Limit to 100 files as per requirement

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'end_date': datetime(2024, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_file_list(date: datetime) -> List[str]:
    """Get list of files for the first day of the month."""
    # Only process first day of each month
    if date.day != 1:
        return []
    
    base_url = "https://samples.adsbexchange.com/readsb-hist"
    date_str = date.strftime('%Y/%m/%d')
    
    files = []
    for hour in range(24):
        for minute in range(0, 60, 2):  # Files every 2 minutes
            if len(files) >= MAX_FILES:
                break
            file_name = f"{hour:02d}{minute:02d}Z.json.gz"
            files.append(f"{base_url}/{date_str}/{file_name}")
    
    return files[:MAX_FILES]

def download_files(**context):
    """Download readsb-hist files and store in S3 raw layer."""
    execution_date = context['execution_date']
    s3 = boto3.client('s3')
    
    files = get_file_list(execution_date)
    if not files:
        logging.info(f"No files to process for date {execution_date}")
        return []
    
    downloaded_files = []
    for file_url in files:
        try:
            response = requests.get(file_url)
            if response.status_code == 200:
                file_name = file_url.split('/')[-1]
                date_path = execution_date.strftime('%Y/%m/%d')
                key = f'raw/readsb_hist/{date_path}/{file_name}'
                
                s3.put_object(
                    Bucket=AWS_BUCKET,
                    Key=key,
                    Body=response.content
                )
                downloaded_files.append(key)
                logging.info(f"Downloaded {file_url} to {key}")
            else:
                logging.warning(f"Failed to download {file_url}: {response.status_code}")
        except Exception as e:
            logging.error(f"Error downloading {file_url}: {str(e)}")
    
    return downloaded_files

def process_files(**context):
    """Process downloaded files and store in prepared layer."""
    execution_date = context['execution_date']
    s3 = boto3.client('s3')
    date_path = execution_date.strftime('%Y/%m/%d')
    
    # List all files in raw layer for this date
    paginator = s3.get_paginator('list_objects_v2')
    raw_files = []
    for page in paginator.paginate(Bucket=AWS_BUCKET, Prefix=f'raw/readsb_hist/{date_path}/'):
        for obj in page.get('Contents', []):
            raw_files.append(obj['Key'])
    
    processed_data = []
    for raw_file in raw_files:
        try:
            # Get raw data
            response = s3.get_object(Bucket=AWS_BUCKET, Key=raw_file)
            data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Process each aircraft in the file
            for aircraft in data.get('aircraft', []):
                if 'hex' in aircraft:  # Only process if we have an identifier
                    processed_aircraft = {
                        'timestamp': data.get('now', 0),
                        'icao': aircraft['hex'],
                        'latitude': aircraft.get('lat'),
                        'longitude': aircraft.get('lon'),
                        'altitude': aircraft.get('alt_baro'),
                        'speed': aircraft.get('gs'),
                        'track': aircraft.get('track'),
                        'vertical_rate': aircraft.get('baro_rate'),
                        'squawk': aircraft.get('squawk'),
                        'emergency': aircraft.get('emergency'),
                        'category': aircraft.get('category'),
                        'source_file': raw_file
                    }
                    processed_data.append(processed_aircraft)
        except Exception as e:
            logging.error(f"Error processing {raw_file}: {str(e)}")
    
    if processed_data:
        # Convert to DataFrame for easier handling
        df = pd.DataFrame(processed_data)
        
        # Store in prepared layer
        prepared_key = f'prepared/readsb_hist/{date_path}/data.parquet'
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer)
        
        s3.put_object(
            Bucket=AWS_BUCKET,
            Key=prepared_key,
            Body=parquet_buffer.getvalue()
        )
        
        return f's3://{AWS_BUCKET}/{prepared_key}'
    
    return None

with DAG(
    'readsb_hist_processing',
    default_args=default_args,
    description='Process readsb-hist data with idempotency',
    schedule_interval='@daily',
    catchup=True,  # Enable catchup to process historical dates
    max_active_runs=1,  # Process one day at a time
    tags=['aircraft', 'tracking', 'readsb']
) as dag:

    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_files,
    )

    process_task = PythonOperator(
        task_id='process_files',
        python_callable=process_files,
    )

    download_task >> process_task
