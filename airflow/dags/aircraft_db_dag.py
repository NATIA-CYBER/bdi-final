import json
import os
import time
from datetime import datetime, timedelta

import boto3
import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

from airflow import DAG
from airflow.operators.python import PythonOperator

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
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_aircraft_db(**context):
    """Download aircraft database from ADSB Exchange."""
    url = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Decompress gzip content and process JSONL format
            import gzip
            import io
            data = []
            with io.BytesIO(response.content) as compressed:
                with gzip.GzipFile(fileobj=compressed) as gzipped:
                    for line in gzipped:
                        if line.strip():  # Skip empty lines
                            try:
                                aircraft = json.loads(line)
                                data.append(aircraft)
                            except json.JSONDecodeError:
                                continue  # Skip invalid JSON lines
            
            if not data:  # If no valid data was found
                raise Exception("No valid aircraft data found in the response")
            
            context['task_instance'].xcom_push(key='raw_data', value=data)
            return 'success'
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:  # Last attempt
                raise Exception(f"Failed to download aircraft data after {max_retries} attempts: {str(e)}")
            time.sleep(retry_delay)
        except Exception as e:
            raise Exception(f"Error processing aircraft data: {str(e)}")

def process_aircraft_db(**context):
    """Transform raw aircraft data and limit to 100 records."""
    data = context['task_instance'].xcom_pull(key='raw_data', task_ids='download_aircraft_db')
    
    # Process data (normalize and clean)
    processed_data = []
    count = 0
    for aircraft in data:
        if count >= 100:  # Limit to 100 records
            break
            
        if aircraft.get('icao', '').strip():  # Only process aircraft with valid ICAO
            processed_aircraft = {
                'icao': aircraft.get('icao', '').strip().upper(),
                'registration': aircraft.get('r', '').strip(),  # Registration is under 'r'
                'manufacturer': aircraft.get('t', '').split(' ')[0] if aircraft.get('t') else '',  # Manufacturer from type
                'model': aircraft.get('t', ''),  # Full type string as model
                'type_code': aircraft.get('type', '').strip().upper(),  # Aircraft type code
                'processed_at': datetime.now().isoformat(),
                'source': 'adsbexchange'
            }
            processed_data.append(processed_aircraft)
            count += 1
    
    context['task_instance'].xcom_push(key='processed_data', value=processed_data)
    return 'success'

def load_to_postgres(**context):
    """Load processed aircraft data to PostgreSQL with idempotency."""
    data = context['task_instance'].xcom_pull(key='processed_data', task_ids='process_aircraft_db')
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host='postgres',  # Using Docker service name
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cur = conn.cursor()
    
    # Create table if not exists with composite primary key for historical data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aircraft (
            icao VARCHAR(24),
            registration VARCHAR(255),
            manufacturer VARCHAR(255),
            model VARCHAR(255),
            type_code VARCHAR(255),
            recorded_time TIMESTAMP WITH TIME ZONE,
            source VARCHAR(255),
            PRIMARY KEY (icao, recorded_time)
        )
    """)
    
    # Insert new records (no upsert needed as we're keeping history)
    insert_query = """
        INSERT INTO aircraft (
            icao, registration, manufacturer, model,
            type_code, recorded_time, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    # Convert data to tuples for batch insert with current timestamp
    current_time = datetime.now()
    records = [(
        aircraft['icao'],
        aircraft['registration'],
        aircraft['manufacturer'],
        aircraft['model'],
        aircraft['type_code'],
        current_time,
        aircraft['source']
    ) for aircraft in data]
    
    # Execute batch upsert
    execute_batch(cur, insert_query, records, page_size=1000)
    
    # Commit and close
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    'aircraft_database',
    default_args=default_args,
    description='Download and process aircraft database with idempotency',
    schedule_interval='@daily',
    catchup=False,
    tags=['aircraft', 'database']
) as dag:

    download_task = PythonOperator(
        task_id='download_aircraft_db',
        python_callable=download_aircraft_db,
    )

    process_task = PythonOperator(
        task_id='process_aircraft_db',
        python_callable=process_aircraft_db,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    download_task >> process_task >> load_task
