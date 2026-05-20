# Real-Time Fake News Detection System

A real-time big data pipeline that fetches Reddit news posts, streams them into Kafka, processes them using Spark Structured Streaming, and stores processed records in HBase.

## Pipeline

```text
Reddit Public Feeds
        ↓
Java Kafka Producer
        ↓
Apache Kafka
        ↓
Spark Structured Streaming
        ↓
HBase
```

## Technologies

- Java 17
- Maven
- Docker Compose
- Apache Kafka
- Apache Spark
- Apache HBase
- Kafka UI

## First-Time Setup

Clone the repository:

```bash
git clone https://github.com/tanushaayer/real-time-fake-news-detection-system.git
cd real-time-fake-news-detection-system
```

Build the project JAR:

```bash
cd pipeline
mvn clean package
cd ..
```

Start all services:

```bash
docker compose down -v
docker compose up --build
```

This starts:

- Zookeeper
- Kafka
- Kafka UI
- Reddit producer
- Spark master
- Spark worker
- Spark consumer job
- HBase

## Useful URLs

Spark UI:

```text
http://localhost:8080
```

Kafka UI:

```text
http://localhost:8090
```

HBase UI:

```text
http://localhost:16010
```

## Create HBase Table

Open HBase shell:

```bash
docker exec -it hbase hbase shell
```

Create table:

```ruby
create 'news_events', 'info'
```

Exit:

```ruby
exit
```

If the table already exists, skip this step.

## Check Producer Logs

```bash
docker logs reddit-producer -f
```

Expected output:

```text
Sent to Kafka [news]: ...
Sent to Kafka [worldnews]: ...
Sent to Kafka [technology]: ...
Sent to Kafka [politics]: ...
```

## Check Spark Consumer Logs

```bash
docker logs spark-consumer -f
```

Expected output:

```text
Processing Spark batch: 0
Wrote records to HBase.
```

## View Kafka Messages

Open Kafka UI:

```text
http://localhost:8090
```

Go to:

```text
Topics → news-stream → Messages
```

## Verify HBase Records

Open HBase shell:

```bash
docker exec -it hbase hbase shell
```

Scan stored records:

```ruby
scan 'news_events', {LIMIT => 10}
```

## Stop Project

```bash
docker compose down
```

Reset all data:

```bash
docker compose down -v
```
