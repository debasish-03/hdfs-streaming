package com.hdfsdemo.hdfsdemo.downloader;


import com.hdfsdemo.hdfsdemo.util.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.net.http.HttpResponse;

@Component
public class HttpCsvDownloader implements CsvDownloader {

    private static final Logger logger = LoggerFactory.getLogger(HttpCsvDownloader.class);

    @Override
    public InputStream download(String url) throws Exception {
        logger.info("Downloading CSV from: {}", url);

        var request = HttpUtils.buildGetRequest(url);
        var response = HttpUtils.getHttpClient().send(request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to download CSV. HTTP status: " + response.statusCode());
        }

        logger.info("Successfully downloaded CSV from: {}", url);
        return response.body();
    }
}
