import os
import time
from datetime import datetime

import happybase
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="Real-Time Fake News Detection Dashboard",
    page_icon="📰",
    layout="wide"
)

HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))
TABLE_NAME = os.getenv("HBASE_TABLE", "news_events")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "10"))


def decode(value):
    if value is None:
        return ""
    return value.decode("utf-8", errors="ignore")


def normalize_label(label):
    label = str(label).upper().strip()

    if label in ["LABEL_0", "REAL", "TRUE"]:
        return "REAL"

    if label in ["LABEL_1", "FAKE", "FALSE"]:
        return "FAKE"

    return label or "UNKNOWN"


@st.cache_data(ttl=5)
def load_hbase_data(limit=300):
    rows = []

    try:
        connection = happybase.Connection(
            host="hbase",
            port=9090,
            timeout=5000
)
        table = connection.table(TABLE_NAME)

        for key, data in table.scan(limit=limit):
            record = {
                "id": key.decode("utf-8", errors="ignore"),
                "source": decode(data.get(b"info:source")),
                "subreddit": decode(data.get(b"info:subreddit")),
                "title": decode(data.get(b"info:title")),
                "author": decode(data.get(b"info:author")),
                "score": decode(data.get(b"info:score")),
                "num_comments": decode(data.get(b"info:num_comments")),
                "url": decode(data.get(b"info:url")),
                "permalink": decode(data.get(b"info:permalink")),
                "ingested_at": decode(data.get(b"info:ingested_at")),
                "prediction_label": normalize_label(
                    decode(data.get(b"info:prediction_label"))
                ),
                "prediction_confidence": decode(
                    data.get(b"info:prediction_confidence")
                ),
            }
            rows.append(record)

        connection.close()

    except Exception as e:
        st.error(f"Could not read from HBase: {e}")

    df = pd.DataFrame(rows)

    if not df.empty:
        df["prediction_confidence"] = pd.to_numeric(
            df["prediction_confidence"],
            errors="coerce"
        ).fillna(0)

        df["score"] = pd.to_numeric(
            df["score"],
            errors="coerce"
        ).fillna(0)

        df["num_comments"] = pd.to_numeric(
            df["num_comments"],
            errors="coerce"
        ).fillna(0)

        df["ingested_at_dt"] = pd.to_datetime(
            df["ingested_at"],
            errors="coerce"
        )

        df = df.sort_values(
            by="ingested_at_dt",
            ascending=False,
            na_position="last"
        )

    return df


st.title("📰 Real-Time Fake News Detection Dashboard")
st.caption("Reddit → Kafka → Spark Structured Streaming → NLP Classifier → HBase → Dashboard")

with st.sidebar:
    st.header("Controls")
    auto_refresh = st.toggle("Auto refresh", value=True)
    refresh_seconds = st.slider("Refresh interval", 5, 60, REFRESH_SECONDS)
    row_limit = st.slider("Rows to load", 50, 1000, 300)

    if st.button("Refresh now"):
        st.cache_data.clear()
        st.rerun()

df = load_hbase_data(row_limit)

if df.empty:
    st.warning("No HBase records found yet. Make sure Spark has written rows to `news_events`.")
    st.stop()

labels = sorted(df["prediction_label"].dropna().unique().tolist())
subreddits = sorted(df["subreddit"].dropna().unique().tolist())

with st.sidebar:
    selected_labels = st.multiselect(
        "Prediction label",
        labels,
        default=labels
    )

    selected_subreddits = st.multiselect(
        "Subreddit",
        subreddits,
        default=subreddits
    )

    min_confidence = st.slider(
        "Minimum confidence",
        0.0,
        1.0,
        0.0,
        0.01
    )

filtered = df[
    df["prediction_label"].isin(selected_labels)
    & df["subreddit"].isin(selected_subreddits)
    & (df["prediction_confidence"] >= min_confidence)
]

total = len(filtered)
fake_count = len(filtered[filtered["prediction_label"] == "FAKE"])
real_count = len(filtered[filtered["prediction_label"] == "REAL"])
avg_confidence = filtered["prediction_confidence"].mean() if total else 0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Records", total)
m2.metric("Fake", fake_count)
m3.metric("Real", real_count)
m4.metric("Avg Confidence", f"{avg_confidence:.2%}")

st.markdown("---")

c1, c2 = st.columns(2)

with c1:
    st.subheader("Prediction Distribution")
    label_counts = filtered["prediction_label"].value_counts().reset_index()
    label_counts.columns = ["Label", "Count"]

    fig = px.pie(
        label_counts,
        names="Label",
        values="Count",
        hole=0.35
    )
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Subreddit Distribution")
    subreddit_counts = filtered["subreddit"].value_counts().head(10).reset_index()
    subreddit_counts.columns = ["Subreddit", "Count"]

    fig = px.bar(
        subreddit_counts,
        x="Subreddit",
        y="Count"
    )
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Confidence by Prediction")
fig = px.histogram(
    filtered,
    x="prediction_confidence",
    color="prediction_label",
    nbins=20
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

st.subheader("Live Classified News Feed")

display_df = filtered[
    [
        "ingested_at",
        "title",
        "subreddit",
        "source",
        "prediction_label",
        "prediction_confidence",
        "score",
        "num_comments",
        "url",
    ]
].copy()

display_df["prediction_confidence"] = display_df["prediction_confidence"].map(
    lambda x: f"{x:.2%}"
)

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True
)

st.markdown("---")

st.subheader("Recent Articles")

for _, row in filtered.head(20).iterrows():
    label = row["prediction_label"]
    confidence = row["prediction_confidence"]

    st.markdown(
        f"""
        **[{label}]** {row['title']}  
        Subreddit: `{row['subreddit']}` | Source: `{row['source']}` | Confidence: `{confidence:.2%}`  
        [Open Article]({row['url']})
        """
    )

if auto_refresh:
    time.sleep(refresh_seconds)
    st.cache_data.clear()
    st.rerun()