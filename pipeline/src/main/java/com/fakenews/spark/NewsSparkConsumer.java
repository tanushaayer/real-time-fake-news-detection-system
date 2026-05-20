package com.fakenews.spark;

import com.fakenews.hbase.HBaseNewsWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.get_json_object;

public class NewsSparkConsumer {

    public static void main(String[] args) throws Exception {

        String kafkaBootstrapServer = "kafka:29092";
        String kafkaTopic = "news-stream";

        System.out.println("Kafka Bootstrap Server: " + kafkaBootstrapServer);
        System.out.println("Kafka Topic: " + kafkaTopic);

        SparkSession spark = SparkSession.builder()
                .appName("FakeNewsSparkStreamingToHBase")
                .master("spark://spark-master:7077")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:29092")
                .option("subscribe", "news-stream")
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> messages = kafkaStream.selectExpr(
                "CAST(value AS STRING) AS json_message"
        );

        Dataset<Row> parsed = messages.select(
                get_json_object(col("json_message"), "$.id").alias("id"),
                get_json_object(col("json_message"), "$.source").alias("source"),
                get_json_object(col("json_message"), "$.subreddit").alias("subreddit"),
                get_json_object(col("json_message"), "$.title").alias("title"),
                get_json_object(col("json_message"), "$.author").alias("author"),
                get_json_object(col("json_message"), "$.score").alias("score"),
                get_json_object(col("json_message"), "$.num_comments").alias("num_comments"),
                get_json_object(col("json_message"), "$.url").alias("url"),
                get_json_object(col("json_message"), "$.permalink").alias("permalink"),
                get_json_object(col("json_message"), "$.ingested_at").alias("ingested_at")
        );

        parsed.writeStream()
                .outputMode("append")
                .foreachBatch((batchDF, batchId) -> {
                    System.out.println("Processing Spark batch: " + batchId);
                    batchDF.foreachPartition(HBaseNewsWriter::writePartition);
                })
                .option("checkpointLocation", "/app/outputs/checkpoints/news-hbase")
                .start()
                .awaitTermination();
    }
}