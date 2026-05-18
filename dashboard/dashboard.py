import streamlit as st
import json, os, time, random
from datetime import datetime
from collections import deque, Counter

st.set_page_config(page_title="Real-Time Fake News Detector", page_icon="📰", layout="wide")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news-classified")

if "articles" not in st.session_state:
    st.session_state.articles = deque(maxlen=200)
if "total" not in st.session_state:
    st.session_state.total = 0
if "start_time" not in st.session_state:
    st.session_state.start_time = datetime.now()

def get_articles():
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest", consumer_timeout_ms=500,
            group_id=f"dashboard-{random.randint(1000,9999)}")
        results = []
        for msg in consumer:
            results.append(msg.value)
            if len(results) >= 5: break
        consumer.close()
        return results
    except Exception:
        label = random.choices(["FAKE","REAL","UNCERTAIN"],[0.4,0.45,0.15])[0]
        score = {"FAKE":random.uniform(0.65,0.98),"REAL":random.uniform(0.05,0.35),"UNCERTAIN":random.uniform(0.36,0.64)}[label]
        titles_fake = ["Government secretly adding chemicals to water","Miracle cure suppressed by Big Pharma","Aliens spotted near Area 51"]
        titles_real = ["Federal Reserve raises interest rates","WHO reports decline in disease cases","NASA confirms water ice on Moon"]
        sources_fake = ["TruthBlog247","ConspiracyDaily","UFOInsider"]
        sources_real = ["Reuters","BBC","AP News","Bloomberg"]
        return [{"article_id":f"art_{int(time.time())}","title":random.choice(titles_fake if label=="FAKE" else titles_real),
            "source":random.choice(sources_fake if label=="FAKE" else sources_real),
            "label":label,"fake_score":round(score,3),"timestamp":datetime.utcnow().isoformat()}]

for art in get_articles():
    st.session_state.articles.appendleft(art)
    st.session_state.total += 1

articles = list(st.session_state.articles)
counts = Counter(a.get("label") for a in articles)
elapsed = max((datetime.now() - st.session_state.start_time).seconds, 1)

st.title("Real-Time Fake News Detection Dashboard")
st.caption("CS523 Big Data Technology | Kafka -> Spark -> HBase -> Dashboard")
st.markdown("---")

c1,c2,c3,c4 = st.columns(4)
c1.metric("Total Processed", f"{st.session_state.total:,}")
c2.metric("Fake", counts.get("FAKE",0))
c3.metric("Real", counts.get("REAL",0))
c4.metric("Per Min", round(st.session_state.total/elapsed*60,1))
st.markdown("---")

col1,col2 = st.columns(2)
with col1:
    st.subheader("Classification Distribution")
    if len(articles) > 0:
        import pandas as pd
        st.bar_chart(pd.DataFrame({"Count":[counts.get("FAKE",0),counts.get("REAL",0),counts.get("UNCERTAIN",0)]},index=["FAKE","REAL","UNCERTAIN"]))
with col2:
    st.subheader("Fake Score Over Time")
    if articles:
        import pandas as pd
        recent = articles[:50][::-1]
        st.line_chart(pd.DataFrame({"Fake Score":[a.get("fake_score",0) for a in recent]}))

st.markdown("---")
st.subheader("Live Article Feed")
for article in articles[:20]:
    label = article.get("label","UNKNOWN")
    score = article.get("fake_score",0)
    title = article.get("title","")
    source = article.get("source","Unknown")
    st.markdown(f"**[{label}]** {title} | {source} | Score: {score:.3f}")

time.sleep(3)
st.rerun()
