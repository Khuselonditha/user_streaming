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
    s_conn = None

    try:
        s_conn = (SparkSession.builder
                    .appName("SparkUserstreaming")
                    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1",
                                                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4")
                    .config("spark.cassandra.connection.host", "localhost")
                    .getOrCreate())

        s_conn.sparkContext.setLog.Level("Error")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark connection due to {e}")

    return s_conn

def create_cassandra_connection():
    """Create a cassandra connection"""
    try:
        cluster = Cluster(["localhost"])

        cass_session = cluster.connect()
        return cass_session
    except Exception as e:
        logging.error(f"Couldn't create the spark connection due to {e}")
        return None


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)