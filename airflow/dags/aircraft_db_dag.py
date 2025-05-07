from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
import gzip
import json
import io
import os
from dotenv import load_dotenv
import psycopg2
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
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_aircraft_db(**context):
    """Download aircraft database and store in S3 raw layer."""
    url = "https://s3.opensky-network.org/data-samples/metadata/aircraftDatabase.json"
    response = requests.get(url)
    data = response.json()
    
    # Store in S3 raw layer with timestamp
    s3 = boto3.client('s3')
    timestamp = context['execution_date'].strftime('%Y/%m/%d')
    
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=f'raw/aircraft_db/{timestamp}/data.json',
        Body=json.dumps(data),
        ContentType='application/json'
    )
    
    return f's3://{AWS_BUCKET}/raw/aircraft_db/{timestamp}/data.json'

def process_aircraft_db(**context):
    """Transform raw aircraft data and store in prepared layer."""
    s3 = boto3.client('s3')
    timestamp = context['execution_date'].strftime('%Y/%m/%d')
    raw_key = f'raw/aircraft_db/{timestamp}/data.json'
    
    # Get raw data
    response = s3.get_object(Bucket=AWS_BUCKET, Key=raw_key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    
    # Process data (normalize and clean)
    processed_data = []
    for aircraft in data:
        processed_aircraft = {
            'icao': aircraft.get('icao', ''),
            'registration': aircraft.get('registration', ''),
            'manufacturer': aircraft.get('manufacturer', ''),
            'model': aircraft.get('model', ''),
            'type_code': aircraft.get('type', ''),
            'processed_at': datetime.now().isoformat(),
            'source': 'adsbexchange'
        }
        processed_data.append(processed_aircraft)
    
    # Store in prepared layer
    prepared_key = f'prepared/aircraft_db/{timestamp}/data.json'
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=prepared_key,
        Body=json.dumps(processed_data),
        ContentType='application/json'
    )
    
    return f's3://{AWS_BUCKET}/{prepared_key}'

def load_to_postgres(**context):
    """Load processed aircraft data to PostgreSQL with idempotency."""
    s3 = boto3.client('s3')
    timestamp = context['execution_date'].strftime('%Y/%m/%d')
    prepared_key = f'prepared/aircraft_db/{timestamp}/data.json'
    
    # Get prepared data
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
        CREATE TABLE IF NOT EXISTS aircraft (
            icao VARCHAR(24) PRIMARY KEY,
            registration VARCHAR(255),
            manufacturer VARCHAR(255),
            model VARCHAR(255),
            type_code VARCHAR(255),
            last_updated TIMESTAMP WITH TIME ZONE,
            source VARCHAR(255)
        )
    """)
    
    # Prepare data for upsert
    insert_query = """
        INSERT INTO aircraft (icao, registration, manufacturer, model, type_code, last_updated, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (icao) 
        DO UPDATE SET
            registration = EXCLUDED.registration,
            manufacturer = EXCLUDED.manufacturer,
            model = EXCLUDED.model,
            type_code = EXCLUDED.type_code,
            last_updated = EXCLUDED.last_updated,
            source = EXCLUDED.source
    """
    
    # Convert data to tuples for batch insert
    records = [(
        aircraft['icao'],
        aircraft['registration'],
        aircraft['manufacturer'],
        aircraft['model'],
        aircraft['type_code'],
        datetime.now(),
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
