import json
import os
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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),  # Start from May 1st
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_fuel_data(**context):
    """Download aircraft fuel consumption rates from GitHub."""
    url = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download fuel consumption data: {response.status_code}")
    
    data = response.json()
    
    # Store in S3
    s3 = boto3.client('s3')
    timestamp = context['execution_date'].strftime('%Y/%m/%d')
    
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=f'raw/fuel_consumption/{timestamp}/data.json',
        Body=json.dumps(data),
        ContentType='application/json'
    )
    
    context['task_instance'].xcom_push(key='raw_data', value=data)
    return 'success'

def process_fuel_data(**context):
    """Process fuel consumption data and load to PostgreSQL."""
    data = context['task_instance'].xcom_pull(key='raw_data', task_ids='download_fuel_data')
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host='postgres',  # Using Docker service name
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cur = conn.cursor()
    
    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fuel_consumption (
            aircraft_type VARCHAR(255) PRIMARY KEY,
            fuel_rate DECIMAL(10,2),
            last_updated TIMESTAMP WITH TIME ZONE
        )
    """)
    
    # Prepare data for upsert
    insert_query = """
        INSERT INTO fuel_consumption (aircraft_type, fuel_rate, last_updated)
        VALUES (%s, %s, %s)
        ON CONFLICT (aircraft_type) 
        DO UPDATE SET
            fuel_rate = EXCLUDED.fuel_rate,
            last_updated = EXCLUDED.last_updated
    """
    
    # Convert data to tuples for batch insert
    records = [(
        aircraft_type,
        float(fuel_rate),
        datetime.now()
    ) for aircraft_type, fuel_rate in data.items()]
    
    # Execute batch upsert
    execute_batch(cur, insert_query, records, page_size=1000)
    
    # Commit and close
    conn.commit()
    cur.close()
    conn.close()
    
    return 'success'

with DAG(
    'fuel_consumption',
    default_args=default_args,
    description='Download and process aircraft fuel consumption rates',
    schedule='@daily',
    catchup=True,  # Enable catchup for backfilling
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
