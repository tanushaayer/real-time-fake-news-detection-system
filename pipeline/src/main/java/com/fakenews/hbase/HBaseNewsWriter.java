package com.fakenews.hbase;

import com.fakenews.nlp.NlpClassifierClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HBaseNewsWriter implements Serializable {

    private static final String TABLE_NAME = "news_events";
    private static final String COLUMN_FAMILY = "info";

    public static void writePartition(Iterator<Row> rows) throws Exception {

        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", "hbase");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(config);

        Table table = connection.getTable(
                TableName.valueOf(TABLE_NAME)
        );

        List<Put> puts = new ArrayList<>();

        while (rows.hasNext()) {

            Row row = rows.next();

            String id = getString(row, "id");

            if (id == null || id.isEmpty()) {
                continue;
            }

            String title = getString(row, "title");

            NlpClassifierClient.ClassificationResult prediction =
                    NlpClassifierClient.classify(title);

            Put put = new Put(Bytes.toBytes(id));

            addColumn(put, "id", id);
            addColumn(put, "source", getString(row, "source"));
            addColumn(put, "subreddit", getString(row, "subreddit"));
            addColumn(put, "title", title);
            addColumn(put, "author", getString(row, "author"));
            addColumn(put, "score", getString(row, "score"));
            addColumn(put, "num_comments", getString(row, "num_comments"));
            addColumn(put, "url", getString(row, "url"));
            addColumn(put, "permalink", getString(row, "permalink"));
            addColumn(put, "ingested_at", getString(row, "ingested_at"));

            addColumn(put, "prediction_label", prediction.label);

            addColumn(
                    put,
                    "prediction_confidence",
                    String.valueOf(prediction.confidence)
            );

            puts.add(put);
        }

        if (!puts.isEmpty()) {
            table.put(puts);

            System.out.println(
                    "Wrote " + puts.size() + " records to HBase."
            );
        }

        table.close();
        connection.close();
    }

    private static void addColumn(
            Put put,
            String column,
            String value
    ) {

        if (value != null) {

            put.addColumn(
                    Bytes.toBytes(COLUMN_FAMILY),
                    Bytes.toBytes(column),
                    Bytes.toBytes(value)
            );
        }
    }

    private static String getString(Row row, String fieldName) {

        try {

            Object value = row.getAs(fieldName);

            return value == null
                    ? ""
                    : value.toString();

        } catch (Exception e) {

            return "";
        }
    }
}