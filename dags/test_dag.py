import requests
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException

# Replace these with your actual Cube.js API endpoints and authentication details
CUBEJS_PREAGGREGATION_URL = 'http://cubeprod-in.unicommerce.infra:4000/cubejs-api/v1/pre-aggregations/jobs'
CUBEJS_API_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MTc2NTU1Mzh9.bW3yk8-Mn6XCGSGI5CmCHO8tl0b_BgHhMx7yxWiTWo'

def trigger_preaggregation():
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {CUBEJS_API_TOKEN}'
    }
    payload = {
    "action": "post",
    "selector": {
        "contexts": [{"securityContext": {}}],
        "timezones": ["UTC"],
        "preAggregations": ["customer_returns.return_overall"]
    }
}
    response = requests.post(CUBEJS_PREAGGREGATION_URL, headers=headers,data=json.dumps(payload), verify=False)
    if response.status_code != 200:
        raise AirflowException(f"Failed to trigger pre-aggregation: {response.text}")
    return response.json()

# def check_preaggregation_status(task_instance):
#     headers = {
#         'Content-Type': 'application/json',
#         'Authorization': f'Bearer {CUBEJS_API_TOKEN}'
#     }
#     response = requests.get(CUBEJS_PREAGGREGATION_STATUS_URL, headers=headers)
#     if response.status_code != 200:
#         raise AirflowException(f"Failed to check pre-aggregation status: {response.text}")

#     status = response.json()
#     if status['status'] != 'done':
#         raise AirflowException(f"Pre-aggregation not completed: {status['status']}")
    
#     task_instance.xcom_push(key='preaggregation_status', value=status)

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
    dag=dag,
)

# check_preaggregation_status_task = PythonOperator(
#     task_id='check_preaggregation_status',
#     python_callable=check_preaggregation_status,
#     provide_context=True,
#     dag=dag,
# )

trigger_preaggregation_task
