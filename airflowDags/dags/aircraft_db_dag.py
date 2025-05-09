import json
import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


# Data validation functions
def validate_icao(icao: str) -> bool:
    """Validate ICAO address format."""
    return bool(re.match(r'^[A-F0-9]{6}$', icao.upper()))

def validate_registration(reg: str) -> bool:
    """Validate aircraft registration format."""
    return bool(re.match(r'^[A-Z0-9-]+$', reg.upper()))

def validate_aircraft_data(aircraft: Dict) -> Optional[Dict]:
    """Validate and clean aircraft data."""
    if not aircraft.get('icao'):
        return None
        
    icao = aircraft['icao'].strip().upper()
    if not validate_icao(icao):
        return None
        
    registration = aircraft.get('r', '').strip().upper()
    if registration and not validate_registration(registration):
        registration = ''
        
    type_code = aircraft.get('type', '').strip().upper()
    if len(type_code) > 10:  # Enforce max length
        type_code = type_code[:10]
        
    manufacturer = aircraft.get('t', '').split(' ')[0] if aircraft.get('t') else ''
    if len(manufacturer) > 100:  # Enforce max length
        manufacturer = manufacturer[:100]
        
    model = aircraft.get('t', '')
    if len(model) > 100:  # Enforce max length
        model = model[:100]
        
    return {
        'icao': icao,
        'registration': registration,
        'manufacturer': manufacturer,
        'model': model,
        'type_code': type_code,
        'processed_at': datetime.now().isoformat(),
        'source': 'adsbexchange'
    }

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
    'start_date': datetime(2025, 5, 1),  # Start from May 1st
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
            invalid_count = 0
            total_count = 0
            
            with io.BytesIO(response.content) as compressed:
                with gzip.GzipFile(fileobj=compressed) as gzipped:
                    for line in gzipped:
                        total_count += 1
                        if line.strip():  # Skip empty lines
                            try:
                                aircraft = json.loads(line)
                                if isinstance(aircraft, dict):
                                    data.append(aircraft)
                                else:
                                    invalid_count += 1
                            except json.JSONDecodeError:
                                invalid_count += 1
                                continue
            
            if not data:  # If no valid data was found
                raise AirflowException("No valid aircraft data found in the response")
            
            # Log data quality metrics
            context['task_instance'].xcom_push(
                key='data_quality_metrics',
                value={
                    'total_records': total_count,
                    'valid_records': len(data),
                    'invalid_records': invalid_count,
                    'success_rate': (
                        (len(data) / total_count * 100) if total_count > 0 else 0
                    )
                }
            )
            
            context['task_instance'].xcom_push(key='raw_data', value=data)
            return f'Successfully downloaded {len(data)} records'
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:  # Last attempt
                raise AirflowException(
                    f"Failed to download aircraft after {max_retries} tries: {str(e)}"
                ) from e
            time.sleep(retry_delay)
        except Exception as e:
            raise AirflowException(f"Error processing aircraft data: {str(e)}") from e

def process_aircraft_db(**context):
    """Transform raw aircraft data and limit to 100 records."""
    data = context['task_instance'].xcom_pull(
        key='raw_data',
        task_ids='download_aircraft_db'
    )
    if not data:
        raise AirflowException("No raw data available from download task")
    
    # Process data with validation
    processed_data = []
    invalid_count = 0
    count = 0
    
    for aircraft in data:
        if count >= 100:  # Limit to 100 records
            break
            
        validated_aircraft = validate_aircraft_data(aircraft)
        if validated_aircraft:
            processed_data.append(validated_aircraft)
            count += 1
        else:
            invalid_count += 1
    
    if not processed_data:
        raise AirflowException("No valid aircraft data after processing")
    
    # Log processing metrics
    context['task_instance'].xcom_push(
        key='processing_metrics',
        value={
            'processed_records': len(processed_data),
            'invalid_records': invalid_count,
            'total_input_records': len(data),
            'validation_success_rate': (
                (len(processed_data) / len(data) * 100) if data else 0
            )
        }
    )
    
    context['task_instance'].xcom_push(key='processed_data', value=processed_data)
    return f'Successfully processed {len(processed_data)} records'

def load_to_postgres(**context):
    """Load processed aircraft data to PostgreSQL with idempotency."""
    data = context['task_instance'].xcom_pull(
        key='processed_data',
        task_ids='process_aircraft_db'
    )
    if not data:
        raise AirflowException("No processed data available for loading")
    
    conn = None
    cur = None
    inserted_count = 0
    error_count = 0
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host='postgres',  # Using Docker service name
            database='airflow',
            user='airflow',
            password='airflow'
        )
        cur = conn.cursor()
        
        # Create table if not exists with composite primary key and constraints
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft (
                icao VARCHAR(24) NOT NULL CHECK (icao ~ '^[A-F0-9]{6}$'),
                registration VARCHAR(255) CHECK (
                    registration IS NULL OR registration ~ '^[A-Z0-9-]+$'
                ),
                manufacturer VARCHAR(255),
                model VARCHAR(255),
                type_code VARCHAR(255),
                last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
                source VARCHAR(255) NOT NULL,
                PRIMARY KEY (icao, last_updated)
            )
        """)
        
        # Insert new records (no upsert needed as we're keeping history)
        insert_query = """
            INSERT INTO aircraft (
                icao, registration, manufacturer, model,
                type_code, last_updated, source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        # Use execution date for historical records
        execution_date = context.get('execution_date', datetime.now())
        records = []
        
        # Validate each record before inserting
        for aircraft in data:
            try:
                if not validate_icao(aircraft['icao']):
                    error_count += 1
                    continue
                    
                if (aircraft['registration'] and 
                    not validate_registration(aircraft['registration'])):
                    aircraft['registration'] = None
                
                records.append((
                    aircraft['icao'],
                    aircraft['registration'],
                    (aircraft['manufacturer'][:100] 
                     if aircraft['manufacturer'] else None),
                    aircraft['model'][:100] if aircraft['model'] else None,
                    aircraft['type_code'][:10] if aircraft['type_code'] else None,
                    execution_date,
                    aircraft['source']
                ))
            except (KeyError, ValueError):
                error_count += 1
                continue
        
        if not records:
            raise AirflowException("No valid records to insert")
        
        # Execute batch insert
        execute_batch(cur, insert_query, records, page_size=1000)
        inserted_count = len(records)
        
        # Commit changes
        conn.commit()
        
        # Log database metrics
        context['task_instance'].xcom_push(
            key='database_metrics',
            value={
                'inserted_records': inserted_count,
                'error_records': error_count,
                'total_records': len(data),
                'success_rate': (inserted_count / len(data) * 100) if data else 0
            }
        )
        
        return f'Successfully inserted {inserted_count} records'
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        raise AirflowException(f"Database error: {str(e)}") from e
    except Exception as e:
        if conn:
            conn.rollback()
        raise AirflowException(f"Error loading data: {str(e)}") from e
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

with DAG(
    'aircraft_database',
    default_args=default_args,
    description='Download and process aircraft database with idempotency',
    schedule='0 0 * * *',  # Run at midnight every day
    catchup=True,  # Enable catchup for backfilling
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

    def copy_to_fastapi_db(**context):
        """Copy data to FastAPI's PostgreSQL database"""
        source_conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        source_cur = source_conn.cursor()
        source_cur.execute('SELECT * FROM aircraft')
        records = source_cur.fetchall()
        
        target_conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        target_cur = target_conn.cursor()
        target_cur.execute('''CREATE TABLE IF NOT EXISTS aircraft (
            icao VARCHAR(24) NOT NULL CHECK (icao ~ '^[A-F0-9]{6}$'),
            registration VARCHAR(255),
            manufacturer VARCHAR(255),
            model VARCHAR(255),
            type_code VARCHAR(255),
            last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
            source VARCHAR(255) NOT NULL,
            PRIMARY KEY (icao, last_updated)
        )''')
        
        execute_batch(target_cur,
            'INSERT INTO aircraft VALUES (%s, %s, %s, %s, %s, %s, %s)',
            records
        )
        target_conn.commit()

    copy_task = PythonOperator(
        task_id='copy_to_fastapi_db',
        python_callable=copy_to_fastapi_db,
    )

    download_task >> process_task >> load_task >> copy_task
