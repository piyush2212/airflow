import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sensors import CheckPreaggregationStatusSensor
import json
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import os

# Replace these with your actual Cube.js API endpoints and authentication details
CUBEJS_PREAGGREGATION_URL = os.getenv('CUBEJS_PREAGGREGATION_URL')
CUBEJS_API_TOKEN = os.getenv('CUBEJS_API_TOKEN')

def trigger_preaggregation(**kwargs):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {CUBEJS_API_TOKEN}'
    }
    payload = {
    "action": "post",
    "selector": {
        "contexts": [{"securityContext": {}}],
        "timezones": ["UTC"],
        "cubes": ["customer_returns"]
    }
    }

    response = requests.post(CUBEJS_PREAGGREGATION_URL, headers=headers,data=json.dumps(payload), verify=False)
    if response.status_code != 200:
        raise AirflowException(f"Failed to trigger pre-aggregation: {response.text}")
    response_data = response.json()
    kwargs['task_instance'].xcom_push(key='trigger_response', value=response_data)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cubejs_preaggregation_dag',
    default_args=default_args,
    description='A simple DAG to trigger Cube.js pre-aggregations and check their status',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

trigger_preaggregation_task = PythonOperator(
    task_id='trigger_preaggregation',
    python_callable=trigger_preaggregation,
    provide_context=True,
    dag=dag,
)

check_preaggregation_status_task = CheckPreaggregationStatusSensor(
    task_id='check_preaggregation_status',
    url=CUBEJS_PREAGGREGATION_URL,
    trigger_task_id= 'trigger_preaggregation',
    api_token = CUBEJS_API_TOKEN,
    poke_interval=30,  # Check every 30s
    timeout=60*30,  # Timeout after 0.5 hours
    dag=dag,
)

trigger_preaggregation_task >> check_preaggregation_status_task
