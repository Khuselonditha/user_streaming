import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    """Create keyspace here"""
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_user_streams
        WITH replication = {'class': 'Simple Strategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    """Create a table here"""
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_user_streams.created_users(
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        dob TEXT,
        gender TEXT,
        address TEXT,
        postal_code TEXT,
        username TEXT,
        email TEXT,
        phone TEXT,
        registered_date TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")

def insert_data(session, **kwargs):
    """Insert data here"""
    print("Inserting Data....")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    dob = kwargs.get('dob')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postal_code = kwargs.get('postal_code')
    username = kwargs.get('username')
    email = kwargs.get('email')
    phone = kwargs.get('phone')
    registered_date = kwargs.get('registered_date')
    picture = kwargs.get('picture')

    required_fields = ['id', 'first_name', 'last_name', 'dob', 'gender', 'address',
                       'postal_code', 'username', 'email', 'phone', 'registered_date',
                       'picture']
    
    for field in required_fields:
        if field not in kwargs:
            logging.error(f"Missing required field: {field}")
            return

    try:

        query = """
            INSERT INTO spark_user_streams.created_users(id, first_name, last_name, dob,
                gender, address, postal_code, username, email, phone, registered_date, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        session.execute(query, (user_id, first_name, last_name, dob, gender, address, postal_code,
                          username, email, phone, registered_date, picture))
        
        logging.info(f"Data Inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f"Could not insert data due to: {e}")

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