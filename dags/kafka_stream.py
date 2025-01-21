# imports
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator         # Used to fecth our data

# Setting default args for our DAG
default_args = {
    "owner": 'khuselo',
    "start_date": datetime(2025, 1, 21, 16, 00) 
}

