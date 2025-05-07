from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from datetime import datetime, timedelta
from airflow.models import Variable
from monitoring import DagMonitor
from io import BytesIO
import requests
import boto3
import json
import os
from dotenv import load_dotenv
import pandas as pd
from typing import List
import logging
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()

# Constants
BUCKET_NAME = os.getenv('AWS_BUCKET')
MAX_FILES = 100  # Maximum number of files to process per execution

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=30),
    'end_date': datetime(2024, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def is_first_day_of_month(date):
    """Check if the given date is the first day of a month."""
    return date.day == 1

def get_file_list(execution_date: datetime) -> List[str]:
    """Get list of files to download for a given date."""
    base_url = "https://samples.adsbexchange.com/readsb-hist"
    date_path = execution_date.strftime('%Y/%m/%d')
    url = f"{base_url}/{date_path}"
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f"Failed to get file list from {url}: {response.status_code}")
            return []
        
        # Extract file URLs from HTML content
        files = []
        for line in response.text.split('\n'):
            if '.json.gz' in line and 'href' in line:
                href = line.split('"')[1]
                files.append(f"{url}/{href}")
        
        logging.info(f"Found {len(files)} files for {date_path}")
        return files[:MAX_FILES]
    except Exception as e:
        logging.error(f"Error getting file list: {str(e)}")
        return []

def download_files(**context):
    """Download readsb-hist files and store in S3 raw layer."""
    DagMonitor.log_task_start(context)
    try:
        execution_date = context['execution_date']
        s3 = boto3.client('s3')

        # Skip if not first day of month
        if not is_first_day_of_month(execution_date):
            logging.info(f"Skipping download for {execution_date} - not first day of month")
            DagMonitor.log_task_end(context, 'Skipped - not first day of month')
            return []
        
        logging.info(f"Starting download process for {execution_date.strftime('%Y-%m-%d')}")
        files = get_file_list(execution_date)
        if not files:
            logging.info(f"No files to process for date {execution_date}")
            DagMonitor.log_task_end(context, 'No files to process')
            return []
        
        downloaded_files = []
        success_count = 0
        error_count = 0
        
        for file_url in files:
            try:
                file_name = file_url.split('/')[-1]
                raw_key = f'raw/readsb_hist/{execution_date.strftime("%Y/%m/%d")}/{file_name}'
                
                # Check if file already exists (idempotency)
                try:
                    s3.head_object(Bucket=BUCKET_NAME, Key=raw_key)
                    logging.info(f"File {raw_key} already exists in S3, skipping...")
                    success_count += 1
                    downloaded_files.append(raw_key)
                    continue
                except s3.exceptions.ClientError:
                    pass  # File doesn't exist, proceed with download
                
                # Download file
                response = requests.get(file_url)
                if response.status_code == 200:
                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=raw_key,
                        Body=response.content
                    )
                    logging.info(f"Successfully downloaded and stored {raw_key}")
                    success_count += 1
                    downloaded_files.append(raw_key)
                else:
                    logging.error(f"Failed to download {file_url}: {response.status_code}")
                    error_count += 1
            except Exception as e:
                logging.error(f"Error processing {file_url}: {str(e)}")
                error_count += 1
        
        summary = f"Download Summary:\n" \
                 f"- Total files attempted: {len(files)}\n" \
                 f"- Successfully downloaded: {success_count}\n" \
                 f"- Errors encountered: {error_count}"
        logging.info(summary)
        DagMonitor.log_task_end(context, summary)
        return downloaded_files

    except Exception as e:
        error_msg = f"Error in download_files: {str(e)}\n{traceback.format_exc()}"
        logging.error(error_msg)
        DagMonitor.log_error(context)
        raise


def process_files(**context):
    """Process downloaded files and store in prepared layer."""
    DagMonitor.log_task_start(context)
    try:
        execution_date = context['execution_date']
        s3 = boto3.client('s3')
        date_path = execution_date.strftime('%Y/%m/%d')

        # Skip if not first day of month
        if not is_first_day_of_month(execution_date):
            logging.info(f"Skipping processing for {execution_date} - not first day of month")
            DagMonitor.log_task_end(context, 'Skipped - not first day of month')
            return None

        # Check if already processed (idempotency)
        prepared_key = f'prepared/readsb_hist/{date_path}/data.parquet'
        try:
            s3.head_object(Bucket=BUCKET_NAME, Key=prepared_key)
            logging.info(f"Data for {date_path} already processed, skipping...")
            DagMonitor.log_task_end(context, 'Skipped - already processed')
            return None
        except s3.exceptions.ClientError:
            pass  # File doesn't exist, proceed with processing

        # Get list of downloaded files from previous task
        task_instance = context['task_instance']
        downloaded_files = task_instance.xcom_pull(task_ids='download_files')

        if not downloaded_files:
            logging.info("No files to process")
            DagMonitor.log_task_end(context, 'No files to process')
            return None

        logging.info(f"Processing {len(downloaded_files)} files")
        all_data = []
        success_count = 0
        error_count = 0

        for raw_key in downloaded_files:
            try:
                response = s3.get_object(Bucket=BUCKET_NAME, Key=raw_key)
                data = json.loads(response['Body'].read().decode('utf-8'))
                processed_data = pd.DataFrame(data)
                all_data.append(processed_data)
                success_count += 1
                logging.info(f"Successfully processed {raw_key}")
            except Exception as e:
                error_count += 1
                logging.error(f"Error processing {raw_key}: {str(e)}")

        if not all_data:
            logging.warning("No data was successfully processed")
            DagMonitor.log_task_end(context, 'No data processed successfully')
            return None

        # Combine and save processed data
        final_df = pd.concat(all_data, ignore_index=True)
        parquet_buffer = BytesIO()
        final_df.to_parquet(parquet_buffer)

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=prepared_key,
            Body=parquet_buffer.getvalue()
        )

        summary = f"Processing Summary:\n" \
                 f"- Total files: {len(downloaded_files)}\n" \
                 f"- Successfully processed: {success_count}\n" \
                 f"- Errors encountered: {error_count}\n" \
                 f"- Total rows processed: {len(final_df)}"
        logging.info(summary)
        DagMonitor.log_task_end(context, summary)
        return prepared_key

    except Exception as e:
        error_msg = f'Error in process_files: {str(e)}\n{traceback.format_exc()}'
        logging.error(error_msg)
        DagMonitor.log_error(context)
        raise

with DAG(
    'readsb_hist_processing',
    default_args=default_args,
    description='Process readsb-hist data for first day of each month',
    schedule_interval='@monthly',  # Run monthly since we only need first day of each month
    catchup=True,  # Enable catchup to process historical dates
    max_active_runs=1,  # Process one day at a time
    tags=['aircraft', 'tracking', 'readsb'],
    doc_md="""## Readsb History Processing DAG
    This DAG processes aircraft tracking data from readsb-hist.
    
    ### Schedule
    - Runs monthly to process first day of each month
    - Date range: 2023/11/01 to 2024/11/01
    
    ### Features
    - Downloads exactly 100 files per execution
    - Implements idempotency checks
    - Uses data lake pattern (raw/prepared)
    - Detailed logging for monitoring
    """
) as dag:

    # Start task for logging
    start_task = EmptyOperator(
        task_id='start_processing',
        dag=dag,
    )

    # Download task with clear logging
    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_files,
        provide_context=True,
        doc_md="""### Download Task
        Downloads readsb-hist files for the first day of each month.
        - Limits to 100 files per day
        - Implements idempotency checks
        - Stores in raw data layer
        """
    )

    # Process task with clear logging
    process_task = PythonOperator(
        task_id='process_files',
        python_callable=process_files,
        provide_context=True,
        doc_md="""### Process Task
        Processes downloaded files and stores in prepared layer.
        - Converts to parquet format
        - Implements idempotency checks
        - Extracts relevant aircraft data
        """
    )

    # End task for logging
    end_task = EmptyOperator(
        task_id='end_processing',
        dag=dag,
    )

    # Task dependencies
    start_task >> download_task >> process_task >> end_task
