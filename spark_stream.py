import logging

from cassandra.cluster import Cluster
from cassandra.policies import ConstantReconnectionPolicy
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructField, StructType
import uuid


def create_keyspace(session):
    """
    Creates a keyspace in Cassandra if it does not exist.
    Args:
        session: Cassandra session object.
    Returns:
        None
    """
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_user_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    logging.info("Keyspace created successfully!")

def create_table(session):
    """
    Creates a table in Cassandra if it does not exist.
    Args:
        session: Cassandra session object.
    Returns:
        None
    """
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

    logging.info("Table created successfully!")


def insert_data(session, **kwargs):
    """
    Inserts user data into the Cassandra table.
    Args:
        session: Cassandra session object.
        **kwargs: Dictionary containing user data fields.
    Returns:
        None
    """
    logging.info("Inserting Data....")

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
        user_id = uuid.UUID(user_id)  # This ensures the id is a valid UUID

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
    """
    Create and return a Spark connection.
    Returns:
        SparkSession: A SparkSession instance if successful, otherwise None.
    """
    s_conn = None

    try:
        s_conn = (SparkSession.builder
            .appName('SparkDataStreaming') 
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
            .config('spark.cassandra.connection.host', 'localhost')
            .config("spark.cassandra.connection.port", "9042")
            .getOrCreate())

        s_conn.sparkContext.setLogLevel("Error")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark connection due to {e}")

    return s_conn


def connect_to_kafka(spark_connection):
    """
    Extract and structure data from the Kafka DataFrame.
    Args:
        spark_df (DataFrame): The streaming DataFrame containing Kafka messages.
    Returns:
        DataFrame: A DataFrame with structured data extracted from Kafka.
    """
    spark_df = None

    try:
        logging.info("Attempting to create Kafka DataFrame...")
        spark_df = spark_connection.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    """
    Create and return a Cassandra connection.
    Returns:
        cassandra.cluster.Session: A Cassandra session object if successful, otherwise None.
    """
    try:
        cluster = Cluster(['localhost'])

        cass_session = cluster.connect()
        return cass_session
    except Exception as e:
        logging.error(f"Couldn't create the cassandra connection due to {e}")

        return None


def create_selection_from_kafka(spark_df):
    """
    Extract and structure data from the Kafka DataFrame.
    Args:
        spark_df (DataFrame): The streaming DataFrame containing Kafka messages.
    Returns:
        DataFrame: A DataFrame with structured data extracted from Kafka.
    """
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postal_code", StringType(), False),
        StructField("username", StringType(), False),
        StructField("email", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("picture", StringType(), False)
        ])
    
    sel = (spark_df.selectExpr("CAST(value AS STRING)")
           .select(from_json(col('value'), schema).alias('data')).select("data.*"))
    print(sel)

    return sel


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is None:
        logging.error("Spark connection could not be established. Exiting...")
        exit(1)

    # Connect to Kafka using spark connection
    spark_df = connect_to_kafka(spark_conn)
    if spark_df is None:
        logging.error("Kafka dataframe could not be created. Exiting...")
        exit(1)

    selection_df = create_selection_from_kafka(spark_df)
    session = create_cassandra_connection()

    if session is not None:
        create_keyspace(session)
        create_table(session)

        logging.info("Streaming is being started...")
        streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                           .option('checkpointLocation', '/tmp/checkpoint')
                           .option('keyspace', 'spark_user_streams')
                           .option('table', 'created_users')
                           .start())
        
        streaming_query.awaitTermination()