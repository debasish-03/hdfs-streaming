package com.hdfsdemo.hdfsdemo;


import com.hdfsdemo.hdfsdemo.aop.TrackExecution;
import com.hdfsdemo.hdfsdemo.config.WebHdfsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.time.Duration;
import java.util.List;

@Service
public class Demo {

    private static final Logger logger = LoggerFactory.getLogger(Demo.class);
    private final WebHdfsConfig config;
    private final HttpClient httpClient;

    public Demo(WebHdfsConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }


    @Retryable(value = Exception.class, maxAttempts = 3)
    public void downloadAndUploadCsv() throws Exception {
        logger.info("Downloading CSV from URL: {}", config.downloadUrl);

        HttpRequest getRequest = HttpRequest.newBuilder()
                        .uri(URI.create(config.downloadUrl))
                        .GET()
                        .build();


        HttpResponse<InputStream> csvResponse = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofInputStream());
        logger.info("Downloading CSV Response: {}", csvResponse);

        if (csvResponse.statusCode() != 200) {
            throw new RuntimeException("Failed to download CSV. HTTP status: " + csvResponse.statusCode());
        }

        InputStream csvStream = csvResponse.body();

        logger.info("Initiating HDFS upload to: {}", config.getWebHdfsUrl());
                HttpRequest createRequest = HttpRequest.newBuilder()
                        .uri(URI.create(config.getWebHdfsUrl()))
                        .PUT(BodyPublishers.noBody())
                        .build();

        HttpResponse<Void> createResp = httpClient.send(createRequest, HttpResponse.BodyHandlers.discarding());

        if (createResp.statusCode() != 307) {
            throw new RuntimeException("WebHDFS redirect failed. Status: " + createResp.statusCode());
        }

        // Step 3: Follow redirect and upload
        String rawRedirectUrl = createResp.headers()
                .firstValue("Location")
                .orElseThrow(() -> new RuntimeException("No redirect URL received"));

        URI originalUri = URI.create(rawRedirectUrl);
        URI fixedUri = new URI(
                originalUri.getScheme(),
                null,
                "localhost",  // Replace with reachable DataNode hostname or IP
                originalUri.getPort(),
                originalUri.getPath(),
                originalUri.getQuery(),
                originalUri.getFragment()
        );

        String redirectUrl = fixedUri.toString();
        logger.info("Redirected to upload location: {}", redirectUrl);

        HttpRequest uploadRequest = HttpRequest.newBuilder()
                .uri(URI.create(redirectUrl))
                .PUT(BodyPublishers.ofInputStream(() -> csvStream))
                .build();

        HttpResponse<String> finalResponse = httpClient.send(uploadRequest, HttpResponse.BodyHandlers.ofString());

        int statusCode = finalResponse.statusCode();
        String responseBody = finalResponse.body();

        // Accept HDFS "fake 100" response as success if body contains "201 Created"
        if ((statusCode == 201) ||
                (statusCode == 100 && responseBody != null && responseBody.contains("201 Created"))) {
            logger.info("Upload successful! Status: {}, Body: {}", statusCode, responseBody);
        } else {
            logger.error("Upload failed. Status: {}, Body: {}", statusCode, responseBody);
            throw new RuntimeException("Upload to HDFS failed. Status: " + statusCode);
        }

        logger.info("CSV successfully uploaded to HDFS at: {}", config.targetPath);

    }

    @TrackExecution
    @Retryable(value = Exception.class, maxAttempts = 1)
    public void downloadAndUploadMultipleCsvs() throws Exception {
        logger.info("Fetching CSV URL list from metadata API (hardcoded for now)");

        List<String> downloadUrls = List.of(
                "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD",
                "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
        );

        int counter = 1;
        for (String url : downloadUrls) {
            String dynamicHdfsPath = this.config.targetPath + "file_" + counter + ".csv";
            logger.info("Processing CSV #{}: URL={}, HDFS Path={}", counter, url, dynamicHdfsPath);
            uploadSingleCsv(url, dynamicHdfsPath);
            counter++;
        }
    }


    private void uploadSingleCsv(String downloadUrl, String hdfsPath) throws Exception {
        // Step 2: Download CSV
        HttpRequest getRequest = HttpRequest.newBuilder()
                .uri(URI.create(downloadUrl))
                .GET()
                .build();

        HttpResponse<InputStream> csvResponse = httpClient.send(getRequest, HttpResponse.BodyHandlers.ofInputStream());
        if (csvResponse.statusCode() != 200) {
            throw new RuntimeException("Failed to download CSV. URL: " + downloadUrl + ", Status: " + csvResponse.statusCode());
        }

        InputStream csvStream = csvResponse.body();

        // Step 3: Initiate HDFS Upload (WebHDFS Create)
        String hdfsCreateUrl = config.getWebHdfsBaseUrl() + hdfsPath + "?op=CREATE&user.name=root&overwrite=true";
        logger.info("Initiating HDFS upload to: {}", hdfsCreateUrl);

        HttpRequest createRequest = HttpRequest.newBuilder()
                .uri(URI.create(hdfsCreateUrl))
                .PUT(BodyPublishers.noBody())
                .build();

        HttpResponse<Void> createResp = httpClient.send(createRequest, HttpResponse.BodyHandlers.discarding());
        if (createResp.statusCode() != 307) {
            throw new RuntimeException("WebHDFS redirect failed. Status: " + createResp.statusCode());
        }

        // Step 4: Follow redirect and upload
        String rawRedirectUrl = createResp.headers()
                .firstValue("Location")
                .orElseThrow(() -> new RuntimeException("Missing redirect Location header"));

        URI originalUri = URI.create(rawRedirectUrl);
        URI fixedUri = new URI(
                originalUri.getScheme(),
                null,
                "localhost",  // Replace this with your DataNode hostname
                originalUri.getPort(),
                originalUri.getPath(),
                originalUri.getQuery(),
                originalUri.getFragment()
        );

        HttpRequest uploadRequest = HttpRequest.newBuilder()
                .uri(fixedUri)
                .PUT(BodyPublishers.ofInputStream(() -> csvStream))
                .build();

        HttpResponse<String> finalResponse = httpClient.send(uploadRequest, HttpResponse.BodyHandlers.ofString());
        int statusCode = finalResponse.statusCode();
        String responseBody = finalResponse.body();

        if ((statusCode == 201) || (statusCode == 100 && responseBody != null && responseBody.contains("201 Created"))) {
            logger.info("Upload successful to HDFS path: {}", hdfsPath);
        } else {
            logger.error("Upload failed. Status: {}, Body: {}", statusCode, responseBody);
            throw new RuntimeException("Upload to HDFS failed.");
        }
    }
}

