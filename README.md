# Real-Time Fake News Detection System

A real-time big data pipeline that fetches news posts from Reddit, streams them into Kafka, processes them with Spark Structured Streaming, and stores processed records in HBase.

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

Start all Docker services:

```bash
docker compose down -v
docker compose up --build
```

## Available UIs

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

Wait until HBase finishes starting. Then run:

```bash
docker exec -it hbase hbase shell
```

Inside HBase shell:

```ruby
create 'news_events', 'info'
list
exit
```

## Restart Spark Consumer

After the HBase table is created:

```bash
docker compose restart spark-consumer
```

Check Spark consumer logs:

```bash
docker logs spark-consumer -f
```

Expected output:

```text
Processing Spark batch...
Wrote X records to HBase.
```

## Verify Kafka Messages

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

Scan table:

```ruby
scan 'news_events', {LIMIT => 10}
```

You should see Reddit news records stored in HBase.

## Stop the Project

```bash
docker compose down
```

Reset all data:

```bash
docker compose down -v
```
