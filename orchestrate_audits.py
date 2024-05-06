from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple Airflow DAG',
    schedule_interval='@hourly'
)


def read_watermark():
    # Execute the SQL for reading watermark
    pass


def check_audits(watermark):
    """
    :param watermark: to be used in the SQL queries for effectively
    """
    # Execute the SQL for checking audits
    pass


def write_watermark():
    # Execute the SQL for writing the new watermark to the table
    pass


def identify_missing_records():
    # Execute the SQL to identify the missing records
    # The missing records can be then further ingested in a table for analysis
    pass


start_task = DummyOperator(task_id='start_task', dag=dag)


read_watermark_task = PythonOperator(
    task_id='read_watermark',
    python_callable=read_watermark,
    dag=dag,
)

check_audits_task = PythonOperator(
    task_id='check_audits',
    python_callable=check_audits,
    dag=dag,
)

write_watermark_task = PythonOperator(
    task_id='write_watermark',
    python_callable=write_watermark,
    dag=dag,
)

identify_missing_records = PythonOperator(
    task_id='identify_missing_records',
    python_callable=identify_missing_records,
    dag=dag,
)

end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> read_watermark_task >> check_audits_task >> write_watermark_task >> identify_missing_records >> end_task
