# imports
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator         # Used to fecth our data
from kafka import KafkaProducer
import json
import requests
import logging
import time

# Setting default args for our DAG
default_args = {
    "owner": 'khuselo',
    "start_date": datetime(2025, 1, 28, 17, 00) 
}

# Get data function
def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()['results'][0]

    return res

# format data function
def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['dob'] = res['dob']['date']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}," \
                      f" {location['city']}, {location['state']}, {location['country']}"
    data['postal code'] = res['location']['postcode']
    data['username'] = res['login']['username']
    data['email'] = res['email']
    data['phone'] = res['phone']
    data['registered_date'] = res['registered']['date']
    data['picture'] = res['picture']['medium']

    return data


# Streaming function
def stream_data():
    # create_topic_if_not_exists()  # Ensure the topic exists
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()
    
    try:
        while True:
            if time.time() > current_time + 60: # A minute
                break
            try:
                data = get_data()
                logging.info(f"Fetched data: {data}")
                res = format_data(data)
                logging.info(f"Formatted data: {res}")

                producer.send("user_created", json.dumps(res).encode("utf-8"))
                logging.info("Data successfully sent to Kafka.")
            except Exception as e:
                logging.error(f"An error occured: {e}")
                continue
    finally:
        producer.close()


# Create DAG
with DAG("user_automation",
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id= 'stream_data_from_api',
        python_callable=stream_data
    )
