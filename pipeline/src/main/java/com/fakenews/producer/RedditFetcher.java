package com.fakenews.producer;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class RedditFetcher {

    private static final String REDDIT_URL =
            "https://www.reddit.com/r/news/new.json?limit=50";

    private static final OkHttpClient client = new OkHttpClient();

    // store already printed posts
    private static final Set<String> seenPosts = new HashSet<>();

    public static void main(String[] args) throws Exception {

        System.out.println("Starting Reddit fetcher...");

        while (true) {

            try {

                Request request = new Request.Builder()
                        .url(REDDIT_URL)
                        .header("User-Agent", "fake-news-pipeline")
                        .build();

                Response response = client.newCall(request).execute();

                if (!response.isSuccessful()) {
                    System.out.println("Request failed: " + response.code());
                    Thread.sleep(10000);
                    continue;
                }

                String responseBody = response.body().string();

                JSONObject json = new JSONObject(responseBody);

                JSONArray posts = json
                        .getJSONObject("data")
                        .getJSONArray("children");

                for (int i = 0; i < posts.length(); i++) {

                    JSONObject post =
                            posts.getJSONObject(i).getJSONObject("data");

                    String id = post.getString("id");

                    // avoid duplicates
                    if (seenPosts.contains(id)) {
                        continue;
                    }

                    seenPosts.add(id);

                    String title = post.getString("title");
                    String subreddit = post.getString("subreddit");
                    String author = post.getString("author");

                    System.out.println("\nNEW POST");
                    System.out.println("Title: " + title);
                    System.out.println("Subreddit: " + subreddit);
                    System.out.println("Author: " + author);
                    System.out.println("--------------------------------");

                }

                // wait 10 seconds
                Thread.sleep(10000);

            } catch (Exception e) {

                System.out.println("Error: " + e.getMessage());

                Thread.sleep(10000);
            }
        }
    }
}