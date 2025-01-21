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

# format data function
def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['dob'] = res['dob']['date']
    data['gender'] = res['gender']
    data['address'] = f"{res['location']['street']['number']} {res['location']['street']['name']}, {res['location']['city']}, {res['location']['state']}, {res['location']['country']}"
    data['postal code'] = res['location']['postcode']
    data['username'] = res['login']['username']
    data['email'] = res['email']
    data['phone'] = res['phone']
    data['registered_date'] = res['registered']['date']
    data['picture'] = res['picture']['medium']

    return data

# Streaming function
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

data = get_data()
print(format_data(data))