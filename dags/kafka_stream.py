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


def get_data():
    """
    Fetches random user data from the randomuser.me API.
    Returns:
        dict: A dictionary containing user data.
    """
    res = requests.get('https://randomuser.me/api/')
    res = res.json()['results'][0]

    return res

def format_data(res):
    """
    Formats the raw user data into a structured dictionary.
    Args:
        res (dict): The raw user data.
    Returns:
        dict: A dictionary containing formatted user data.
    """
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['dob'] = res['dob']['date']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}," \
                      f" {location['city']}, {location['state']}, {location['country']}"
    data['postal_code'] = res['location']['postcode']
    data['username'] = res['login']['username']
    data['email'] = res['email']
    data['phone'] = res['phone']
    data['registered_date'] = res['registered']['date']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    """
    Streams formatted user data to a Kafka topic.
    
    This function fetches data from the randomuser.me API, formats it, and sends it to a Kafka topic named 'users_created'.
    The streaming runs for 60 seconds.

    Raises:
        Exception: If there is an error in fetching, formatting, or sending data.
    """
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

                producer.send("users_created", json.dumps(res).encode("utf-8"))
                logging.info("Data successfully sent to Kafka.")
            except Exception as e:
                logging.error(f"An error occured: {e}")
                continue
    finally:
        producer.close()
