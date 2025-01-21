# imports
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator         # Used to fecth our data
import json
import requests

# Setting default args for our DAG
default_args = {
    "owner": 'khuselo',
    "start_date": datetime(2025, 1, 21, 17, 00) 
}

# Get data function
def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()['results'][0]

    return res

# # Streaming function
# def stream_data():
#     print(json.dumps(res, indent=3))

# Create DAG
# with DAG("user_automation",
#         default_args=default_args,
#         schedule_interval='@daily',
#         catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id= 'stream_data_from_api',
#         python_callable=stream_data()
#     )

stream_data()