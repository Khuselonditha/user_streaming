# User Streaming | End-to-End Data Engineering Project

## Table of Contents
- [Overview](#Overview)
- [System Architecture](#system-architecture)
- [What You'll Learn](#what-youll-learn)
- [Technologies](#technologies)
- [Getting Started](#getting-started)
- [Watch the Video Tutorial](#watch-the-video-tutorial)

## Overview
This project implements a real-time user data streaming pipeline using **Apache Spark**, **Apache Kafka**, and **Apache Cassandra**. The pipeline ingests user creation events from Kafka, processes them with Spark Structured Streaming, and stores them in a Cassandra database.

## System Architecture

![System Architecture](https://git@github.com:Khuselonditha/user_streaming.git/Data%20engineering%20architecture.png)

## Folder Structure
```
/home/khuselo/my_work/side_things/user_streaming/
├── jars/                      # Required JARs for Spark
│   ├── spark-cassandra-connector_2.13-3.5.1.jar
│   └── spark-sql-kafka-0-10_2.12-3.5.1.jar
├── scripts/                   # Helper scripts
│   ├── entrypoint.sh          # Shell script for container entrypoint
├── spark_stream.py            # Main Spark streaming script
├── docker-compose.yml         # Docker setup for Kafka, Spark, and Cassandra
├── requirements.txt           # Python dependencies
```

## Dependencies
Make sure you have the following installed:
- **Python 3.8+**
- **Apache Spark 3.5.1**
- **Apache Kafka**
- **Apache Cassandra**
- **Docker & Docker Compose** (for containerized setup)

### Python Packages (Install via `pip`)
```sh
pip install -r requirements.txt
```

## Setup & Usage
### 1. Start Services using Docker
Ensure Docker is installed and run:
```sh
docker-compose up -d
```

### 2. Run Spark Streaming Application
```sh
python spark_stream.py
```

## How It Works
1. **Kafka Topic** (`users_created`) receives user creation events.
2. **Spark Structured Streaming** reads from Kafka, processes the JSON messages, and extracts user details.
3. **Cassandra** stores structured user data in the `spark_user_streams.created_users` table.

## Cassandra Schema
```sql
CREATE TABLE IF NOT EXISTS spark_user_streams.created_users (
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
    picture TEXT
);
```

## Environment Variables
| Variable       | Description                     |
|---------------|---------------------------------|
| KAFKA_BROKER  | Kafka bootstrap servers (e.g. `localhost:9092`) |
| CASSANDRA_HOST| Cassandra host (e.g. `localhost`) |

## Logging & Debugging
To view logs, run:
```sh
tail -f logs/spark_stream.log
```

## Contributing
Feel free to open issues or create pull requests for improvements.

## License
This project is open-source under the MIT License.

