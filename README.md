# User Streaming | End-to-End Data Engineering Project

## Table of Contents
- [Overview](#Overview)
- [System Architecture](#System-Architecture)
- [Technologies](#Technologies)
- [Getting Started](#getting-started)

## Overview
This is an end-to-end data engineering pipeline. It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is containerized using Docker for ease of deployment and scalability.

## System Architecture

![System Architecture](Data_engineering_architecture.png)

The project is designed with the following components:

- **Data Source**: We use `randomuser.me` API to generate random user data for our pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker


## Getting Started

1. Clone the repository:
    ```bash
    git clone git@github.com:Khuselonditha/user_streaming.git
    ```

2. Navigate to the project directory:
    ```bash
    cd user_streaming
    ```

3. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up -d
    ```

4. Run Spark Streaming Application
    ```bash
    python spark_stream.py
    ```

## Contributing
Feel free to open issues or create pull requests for improvements.

## License
This project is open-source under the MIT License.

