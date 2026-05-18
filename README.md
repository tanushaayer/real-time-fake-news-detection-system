1. Docker Setup (docker-compose.yml)
Configured the full Docker infrastructure for the project.
Starts Zookeeper, Kafka, Spark, HBase and Dashboard with one command:
docker compose up --build -d

2. Live Dashboard (dashboard/dashboard.py)
Built a Streamlit web app that shows fake news results in real time.
Displays live metrics, charts and article feed.
Auto-refreshes every 3 seconds.
Open at http://localhost:8501

3. Dashboard Container (dashboard/Dockerfile)
Packaged the dashboard to run inside Docker.

4. Dashboard Dependencies (dashboard/requirements.txt)
Added required Python libraries for the dashboard.
