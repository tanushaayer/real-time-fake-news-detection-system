package com.fakenews.nlp;

import okhttp3.*;
import org.json.JSONObject;

public class NlpClassifierClient {

    private static final String NLP_SERVICE_URL =
            System.getenv().getOrDefault(
                    "NLP_SERVICE_URL",
                    "http://nlp-service:8000/classify"
            );

    private static final OkHttpClient client = new OkHttpClient();

    public static ClassificationResult classify(String text) {
        try {
            if (text == null || text.trim().isEmpty()) {
                return new ClassificationResult("UNKNOWN", 0.0);
            }

            JSONObject json = new JSONObject();
            json.put("text", text);

            MediaType mediaType = MediaType.parse("application/json");

            RequestBody body = RequestBody.create(
                    mediaType,
                    json.toString()
            );;

            Request request = new Request.Builder()
                    .url(NLP_SERVICE_URL)
                    .post(body)
                    .build();

            Response response = client.newCall(request).execute();

            if (!response.isSuccessful() || response.body() == null) {
                return new ClassificationResult("ERROR", 0.0);
            }

            JSONObject result = new JSONObject(response.body().string());

            String label = result.optString("label", "UNKNOWN");
            double confidence = result.optDouble("confidence", 0.0);

            return new ClassificationResult(label, confidence);

        } catch (Exception e) {
            return new ClassificationResult("ERROR", 0.0);
        }
    }

    public static class ClassificationResult {
        public final String label;
        public final double confidence;

        public ClassificationResult(String label, double confidence) {
            this.label = label;
            this.confidence = confidence;
        }
    }
}