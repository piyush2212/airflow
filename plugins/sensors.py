from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
import requests 
import json 

class CheckPreaggregationStatusSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, url,trigger_task_id, api_token,*args, **kwargs):
        super(CheckPreaggregationStatusSensor, self).__init__(*args, **kwargs)
        self.trigger_task_id = trigger_task_id  
        self.api_token = api_token
        self.url = url

    def poke(self, context):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_token}'
        }
        trigger_response = context['task_instance'].xcom_pull(key='trigger_response', task_ids=self.trigger_task_id)
        if not trigger_response:
            raise AirflowException(f"No response from trigger pre-aggregation task: {self.trigger_task_id}")

        for preaggregation_id in trigger_response:
            url = self.url
            payload = {
                    "action": "get",
                    "tokens": trigger_response
                }
            response = requests.post(url, headers=headers, data=json.dumps(payload),verify=False)
            if response.status_code != 200:
                raise AirflowException(f"Failed to check pre-aggregation status for ID {preaggregation_id}: {response.text}")

            status = response.json()
            if status[0]['status'] != 'done':
                return False

        return True
