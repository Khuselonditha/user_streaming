import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    """Create keyspace here"""

def create_table(session):
    """Create a table here"""

def insert_data(session):
    """Insert data here"""

def create_spark_connection():
    """Create spark connection"""

def create_cassandra_connection():
    """Create a cassandra connection"""


if __name__ == "__main__":
    spark_conn = create_spark_connection()