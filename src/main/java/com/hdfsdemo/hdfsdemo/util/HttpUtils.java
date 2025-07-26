package com.hdfsdemo.hdfsdemo.util;


import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;

public class HttpUtils {

    private static final HttpClient DEFAULT_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    public static HttpClient getHttpClient() {
        return DEFAULT_CLIENT;
    }

    public static HttpRequest buildGetRequest(String url) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
    }

    public static HttpRequest buildPutRequest(String url) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build();
    }

    public static HttpRequest buildPutRequestWithBody(String url, InputStream bodyStream) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .PUT(HttpRequest.BodyPublishers.ofInputStream(() -> bodyStream))
                .build();
    }
}
