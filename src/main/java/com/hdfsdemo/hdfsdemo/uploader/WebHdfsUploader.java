package com.hdfsdemo.hdfsdemo.uploader;


import com.hdfsdemo.hdfsdemo.config.WebHdfsConfig;
import com.hdfsdemo.hdfsdemo.exception.CsvUploadException;
import com.hdfsdemo.hdfsdemo.util.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpResponse;

@Component
public class WebHdfsUploader implements HdfsUploader {

    private static final Logger logger = LoggerFactory.getLogger(WebHdfsUploader.class);
    private final WebHdfsConfig config;

    public WebHdfsUploader(WebHdfsConfig config) {
        this.config = config;
    }

    @Override
    public void upload(String hdfsPath, InputStream csvStream) throws Exception {
        String hdfsCreateUrl = config.getWebHdfsBaseUrl() + hdfsPath + "?op=CREATE&user.name=root&overwrite=true";
        logger.info("Initiating HDFS upload to: {}", hdfsCreateUrl);

        var createRequest = HttpUtils.buildPutRequest(hdfsCreateUrl);
        var createResp = HttpUtils.getHttpClient().send(createRequest, HttpResponse.BodyHandlers.discarding());

        if (createResp.statusCode() != 307) {
            throw new RuntimeException("WebHDFS redirect failed. Status: " + createResp.statusCode());
        }

        String rawRedirectUrl = createResp.headers().firstValue("Location")
                .orElseThrow(() -> new RuntimeException("Redirect location missing"));

        URI originalUri = URI.create(rawRedirectUrl);
        URI redirectUri = new URI(
                originalUri.getScheme(),
                null,
                "localhost", // Replace with your actual DataNode host
                originalUri.getPort(),
                originalUri.getPath(),
                originalUri.getQuery(),
                originalUri.getFragment()
        );

        var uploadRequest = HttpUtils.buildPutRequestWithBody(redirectUri.toString(), csvStream);
        var finalResponse = HttpUtils.getHttpClient().send(uploadRequest, HttpResponse.BodyHandlers.ofString());

        int statusCode = finalResponse.statusCode();
        String responseBody = finalResponse.body();

        if ((statusCode == 201) || (statusCode == 100 && responseBody.contains("201 Created"))) {
            logger.info("Upload successful to HDFS path: {}", hdfsPath);
        } else {
            logger.error("Upload failed. Status: {}, Body: {}", statusCode, responseBody);
            throw new CsvUploadException("Upload to HDFS failed.");
        }
    }
}
