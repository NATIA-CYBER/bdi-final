import json
import os
from datetime import datetime, timedelta

import boto3
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

# Load environment variables
load_dotenv()

AWS_BUCKET = os.getenv('AWS_BUCKET')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'end_date': datetime(2024, 11, 1),  # Added end date
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
    """Download readsb-hist files for the first day of each month.
    
    Limited to 100 files per execution.
    """
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
    except Exception as e:
        print(f"Error checking file: {e}")
        pass
    
    all_data = []
    # Download only 100 files
    for i in range(100):
        try:
            url = f"https://samples.adsbexchange.com/readsb-hist/{date_str}/readsb-hist_{i:04d}.json"
            response = requests.get(url)
            if response.status_code == 200:
                file_data = response.json()
                all_data.extend(file_data)
            else:
                print(
                    f"Failed to download file {i}, "
                    f"status code: {response.status_code}"
                )
        except Exception as e:
            print(f"Error downloading file {i}: {e}")
            continue
    
    # Store combined data in S3
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=raw_key,
        Body=json.dumps(all_data),
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
        if all(k in record for k in ['timestamp', 'icao', 'lat', 'lon']):
            processed_record = {
                'timestamp': record.get('timestamp'),
                'icao': record.get('icao'),
                'callsign': record.get('callsign'),
                'latitude': record.get('lat'),
                'longitude': record.get('lon'),
                'altitude': record.get('alt_baro'),
                'ground_speed': record.get('gs'),
                'track': record.get('track'),
                'processed_at': datetime.now().isoformat(),
                'date': execution_date.strftime('%Y-%m-%d')
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

def load_to_postgres(**context):
    """Load processed data to PostgreSQL."""
    execution_date = context['execution_date']
    
    if execution_date.day != 1:
        return "Skipped"
    
    date_str = execution_date.strftime('%Y/%m/%d')
    s3 = boto3.client('s3')
    
    # Get prepared data
    prepared_key = f'prepared/readsb_hist/{date_str}/data.json'
    response = s3.get_object(Bucket=AWS_BUCKET, Key=prepared_key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    
    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aircraft_tracking (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            icao VARCHAR(24),
            callsign VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            altitude FLOAT,
            ground_speed FLOAT,
            track FLOAT,
            processed_at TIMESTAMP WITH TIME ZONE,
            date DATE,
            UNIQUE(icao, timestamp)
        )
    """)
    
    # Prepare data for insert
    insert_query = """
        INSERT INTO aircraft_tracking (
            timestamp, icao, callsign, latitude, longitude,
            altitude, ground_speed, track, processed_at, date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (icao, timestamp) DO NOTHING
    """
    
    # Convert data to tuples for batch insert
    records = [(
        record['timestamp'],
        record['icao'],
        record['callsign'],
        record['latitude'],
        record['longitude'],
        record['altitude'],
        record['ground_speed'],
        record['track'],
        datetime.now(),
        record['date']
    ) for record in data]
    
    # Execute batch insert
    execute_batch(cur, insert_query, records, page_size=1000)
    
    # Commit and close
    conn.commit()
    cur.close()
    conn.close()
    
    return f"Loaded {len(records)} records to PostgreSQL"

with DAG(
    'readsb_history',
    default_args=default_args,
    description='Download and process readsb-hist data for first day of each month',
    schedule_interval='@daily',
    catchup=True,  # Enable catchup to process historical data
    tags=['aircraft', 'history']
) as dag:

    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_files,
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    download_task >> process_task >> load_task
