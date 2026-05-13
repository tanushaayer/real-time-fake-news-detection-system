# Real-Time Fake News Detection System

A real-time big data pipeline that continuously fetches news posts from Reddit and streams them into Apache Kafka for downstream processing and analytics.

## Pipeline

```text
Reddit Feed
    ↓
Java Producer
    ↓
Apache Kafka
```

## Technologies Used

- Java 17
- Apache Kafka
- Docker Compose
- Maven
- Reddit Public JSON Feed

## How to Run

From the root project folder:

```bash
docker compose up --build
```

This will automatically:

- start Zookeeper
- start Kafka
- create the Kafka topic `news-stream`
- start the Java Reddit producer

## View Producer Logs

```bash
docker logs reddit-producer -f
```

## View Kafka Messages

```bash
docker exec -it kafka kafka-console-consumer \
  --topic news-stream \
  --bootstrap-server localhost:9092 \
  --from-beginning
```