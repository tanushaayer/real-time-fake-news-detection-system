# Real-Time Fake News Detection System

A distributed real-time fake news detection pipeline that streams live Reddit news using Apache Kafka, processes data with Spark Structured Streaming, classifies news using an NLP model, stores results in HBase, and visualizes analytics through a Streamlit dashboard.

## Technologies Used

- Java
- Python
- Apache Kafka
- Apache Spark Structured Streaming
- HBase
- FastAPI
- Hugging Face Transformers
- Streamlit
- Docker & Docker Compose
- Reddit API

## Architecture

Reddit API → Kafka Producer → Kafka Topic → Spark Structured Streaming → NLP Classifier → HBase → Streamlit Dashboard

## Running the Project

### 1. Clone the Repository

```bash
git clone https://github.com/tanushaayer/real-time-fake-news-detection-system.git
cd real-time-fake-news-detection-system
```

### 2. Start Docker Containers

```bash
docker compose up --build
```

### 3. Create HBase Table

Open a new terminal:

```bash
docker exec -it hbase hbase shell
```

Inside HBase shell:

```ruby
create 'news_events', 'info'
```

Exit shell:

```ruby
exit
```

### 4. Open Services

| Service | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8090 |
| NLP Service Docs | http://localhost:8000/docs |
| Spark Master UI | http://localhost:8080 |
| HBase UI | http://localhost:16010 |

## Replay Kafka Messages from Beginning

Stop Spark consumer:

```bash
docker stop spark-consumer
```

Delete checkpoint folder:

### Windows PowerShell

```powershell
Remove-Item -Recurse -Force .\outputs\checkpoints\news-hbase
```

### Linux / Mac

```bash
rm -rf outputs/checkpoints/news-hbase
```

Start Spark consumer again:

```bash
docker start spark-consumer
```