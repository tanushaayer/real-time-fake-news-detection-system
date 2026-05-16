# Real-Time Fake News Detection System
CS523 - Big Data Technology | Final Course Project

## Project Overview
End-to-end Big Data pipeline that detects fake news in real time using:
- Apache Kafka (data ingestion from Reddit)
- Apache Spark Structured Streaming (processing and classification)
- HBase (persistent storage)
- Streamlit (live dashboard)

## Project Structure
real-time-fake-news-detection-system/
├── pipeline/
│   ├── src/main/java/com/fakenews/
│   │   └── producer/
│   │       └── RedditFetcher.java     Part 1: Kafka Producer
│   └── pom.xml
├── dashboard/                         Part 4: Streamlit Dashboard
│   ├── dashboard.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
└── README.md

## Prerequisites
- Java 11+
- Maven 3.8+
- Docker Desktop
- Python 3.11+

## How to Run Locally with Docker (Recommended)

Step 1: Clone the repo
git clone https://github.com/tanushaayer/real-time-fake-news-detection-system.git
cd real-time-fake-news-detection-system

Step 2: Start all services
docker compose up --build -d

Step 3: Check all services are running
docker compose ps

Step 4: Open the dashboard
http://localhost:8501

Step 5: Monitor Kafka topics
http://localhost:8090

Step 6: Monitor Spark jobs
http://localhost:8080

Step 7: Stop all services
docker compose down

## How to Run Manually without Docker

Step 1: Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

Step 2: Start Kafka
bin/kafka-server-start.sh config/server.properties

Step 3: Create Kafka topics
kafka-topics --create --topic news-stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --create --topic news-classified --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

Step 4: Build and run the Kafka producer
cd pipeline
mvn clean package
java -cp target/reddit-fetcher-1.0-SNAPSHOT.jar com.fakenews.producer.RedditFetcher

Step 5: Submit Spark Streaming job
spark-submit --class com.fakenews.spark.FakeNewsSparkStreaming --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pipeline/target/reddit-fetcher-1.0-SNAPSHOT.jar

Step 6: Run HBase sink
java -cp target/reddit-fetcher-1.0-SNAPSHOT.jar com.fakenews.hbase.HBaseSink

Step 7: Run the dashboard
cd dashboard
pip install -r requirements.txt
streamlit run dashboard.py

## Services and Ports
Dashboard   http://localhost:8501  Live fake news feed
Kafka UI    http://localhost:8090  Monitor Kafka topics
Spark UI    http://localhost:8080  Monitor Spark jobs
HBase UI    http://localhost:16010 Monitor HBase storage

## Architecture
Reddit API
    |
Kafka Producer RedditFetcher.java
    |
Kafka Topic news-stream
    |
Spark Structured Streaming FakeNewsSparkStreaming.java
    |
HBase Storage HBaseSink.java
    |
Streamlit Dashboard dashboard.py

## Team
CS523 Big Data Technology
Maharishi International University
