package com.fakenews.producer;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Instant;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class RedditFetcher {

    private static final String[] REDDIT_URLS = {
            "https://www.reddit.com/r/news+worldnews+technology+politics/new.json?limit=100",
            "https://www.reddit.com/r/news+worldnews+technology+politics/hot.json?limit=100",
            "https://www.reddit.com/r/news+worldnews+technology+politics/top.json?t=day&limit=100",
            "https://www.reddit.com/r/news+worldnews+technology+politics/top.json?t=week&limit=100"
    };

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static final String KAFKA_BOOTSTRAP_SERVER =
            getEnvOrDefault("KAFKA_BOOTSTRAP_SERVER", "localhost:9092");

    private static final String KAFKA_TOPIC =
            getEnvOrDefault("KAFKA_TOPIC", "news-stream");

    private static final int FETCH_INTERVAL_MS =
            Integer.parseInt(getEnvOrDefault("FETCH_INTERVAL_MS", "10000"));

    private static final OkHttpClient client = new OkHttpClient();
    private static final Set<String> seenPostIds = new HashSet<>();

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Reddit Kafka Producer...");
        System.out.println("Kafka Bootstrap Server: " + KAFKA_BOOTSTRAP_SERVER);
        System.out.println("Kafka Topic: " + KAFKA_TOPIC);
        System.out.println("Fetch Interval: " + FETCH_INTERVAL_MS + " ms");

        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down producer...");
            producer.flush();
            producer.close();
        }));

        while (true) {
            try {
                for (String redditUrl : REDDIT_URLS) {
                    fetchRedditPostsAndSendToKafka(producer, redditUrl);
                }

                Thread.sleep(FETCH_INTERVAL_MS);

            } catch (Exception e) {
                System.out.println("Error while fetching Reddit posts: " + e.getMessage());
                Thread.sleep(FETCH_INTERVAL_MS);
            }
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_BOOTSTRAP_SERVER
        );

        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaProducer<>(properties);
    }

    private static void fetchRedditPostsAndSendToKafka(
            KafkaProducer<String, String> producer,
            String redditUrl
    ) throws Exception {

        System.out.println("Fetching Reddit URL: " + redditUrl);

        Request request = new Request.Builder()
                .url(redditUrl)
                .header("User-Agent", "windows:fake-news-pipeline:v1.0 by tanushaayer")
                .build();

        Response response = client.newCall(request).execute();

        if (!response.isSuccessful()) {
            System.out.println("Reddit request failed with status: " + response.code());
            return;
        }

        if (response.body() == null) {
            System.out.println("Empty response from Reddit.");
            return;
        }

        String responseBody = response.body().string();

        JSONObject jsonObject = new JSONObject(responseBody);

        JSONArray posts = jsonObject
                .getJSONObject("data")
                .getJSONArray("children");

        int sentCount = 0;

        for (int i = posts.length() - 1; i >= 0; i--) {
            JSONObject postData = posts
                    .getJSONObject(i)
                    .getJSONObject("data");

            String postId = postData.optString("id");

            if (postId == null || postId.isEmpty()) {
                continue;
            }

            if (seenPostIds.contains(postId)) {
                continue;
            }

            seenPostIds.add(postId);

            JSONObject event = buildNewsEvent(postData, redditUrl);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            KAFKA_TOPIC,
                            postId,
                            event.toString()
                    );

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Sent to Kafka [" +
                            event.optString("subreddit") + "]: " +
                            event.optString("title"));
                } else {
                    System.out.println("Kafka send failed: " + exception.getMessage());
                }
            });

            sentCount++;
        }

        producer.flush();

        if (sentCount == 0) {
            System.out.println("No new Reddit posts found for this feed.");
        } else {
            System.out.println("Batch complete. Sent " + sentCount + " new posts.");
        }
    }

    private static JSONObject buildNewsEvent(JSONObject postData, String redditUrl) {
        JSONObject event = new JSONObject();

        event.put("id", postData.optString("id"));
        event.put("source", "reddit");
        event.put("subreddit", postData.optString("subreddit"));
        event.put("title", postData.optString("title"));
        event.put("author", postData.optString("author"));
        event.put("score", postData.optInt("score"));
        event.put("num_comments", postData.optInt("num_comments"));
        event.put("url", postData.optString("url"));
        event.put("permalink", "https://www.reddit.com" + postData.optString("permalink"));
        event.put("created_utc", postData.optDouble("created_utc"));
        event.put("ingested_at", Instant.now().toString());
        event.put("feed_url", redditUrl);

        return event;
    }
}