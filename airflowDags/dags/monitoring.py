from airflow.models import Variable
from datetime import datetime
import logging

class DagMonitor:
    @staticmethod
    def log_task_start(context):
        task_instance = context['task_instance']
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        execution_date = context['execution_date']

        logging.info('='*80)
        logging.info(f'Starting Task: {task_id}')
        logging.info(f'DAG: {dag_id}')
        logging.info(f'Execution Date: {execution_date}')
        logging.info('='*80)

    @staticmethod
    def log_task_end(context, result=None):
        task_instance = context['task_instance']
        task_id = task_instance.task_id
        duration = datetime.now() - task_instance.start_date

        logging.info('='*80)
        logging.info(f'Task Completed: {task_id}')
        logging.info(f'Duration: {duration}')
        if result:
            logging.info(f'Result: {result}')
        logging.info('='*80)

    @staticmethod
    def log_error(context):
        task_instance = context['task_instance']
        task_id = task_instance.task_id
        exception = context.get('exception')

        logging.error('='*80)
        logging.error(f'Task Failed: {task_id}')
        logging.error(f'Error: {str(exception)}')
        logging.error('='*80)
