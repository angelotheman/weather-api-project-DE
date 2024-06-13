#!/usr/bin/env python3
"""
Scheduling the entire pipeline
"""
from os import getenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
import requests
import json
from dotenv import load_dotenv


load_dotenv()

default_args = {
    'owner': 'angelotheman',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def send_sms_alert():
    """
    Function to send sms alerts
    """
    quicksend_url = "https://uellosend.com/quicksend/"
    data = {
        'api_key': getenv('SMS_API_KEY'),
        'sender_id': 'AIRFLOW',
        'message': """
        Hello there, your weather scheduling pipeline has run successfully,
        check in with your UI and and get your dashboards.
        Happy working!!!
        """,
        'recipient': getenv('SMS_RECIPIENT'),
    }
    headers = {'Content-type': 'application/json'}
    response = requests.post(quicksend_url, headers=headers, json=data)


# Instantiate the DAG
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A DAG to orchestrate the weather data pipeline',
    schedule_interval='0 0 * * *',
    catchup=False,
)

# Define BashOperator tasks
ingest_task = BashOperator(
    task_id='run_ingest',
    bash_command='python3 ../ingest.py',
    dag=dag,
)

transform_task = BashOperator(
    task_id='run_transform',
    bash_command='python3 ../transformations/transformation.py',
    dag=dag,
)

load_task = BashOperator(
    task_id='run_load',
    bash_command='python3 ../load_to_postgres.py',
    dag=dag,
)

# Python Operator for sending SMS
send_sms_alert_task = PythonOperator(
    task_id='send_sms_alert',
    python_callable=send_sms_alert,
    dag=dag,
)


ingest_task >> transform_task >> load_task >> send_sms_alert_task
