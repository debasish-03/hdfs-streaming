package com.hdfsdemo.hdfsdemo.service;


import com.hdfsdemo.hdfsdemo.config.WebHdfsConfig;
import com.hdfsdemo.hdfsdemo.downloader.CsvDownloader;
import com.hdfsdemo.hdfsdemo.uploader.HdfsUploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.List;

@Service
public class CsvHdfsService {

    private static final Logger logger = LoggerFactory.getLogger(CsvHdfsService.class);

    private final CsvDownloader csvDownloader;
    private final HdfsUploader hdfsUploader;
    private final WebHdfsConfig config;

    public CsvHdfsService(CsvDownloader csvDownloader, HdfsUploader hdfsUploader, WebHdfsConfig config) {
        this.csvDownloader = csvDownloader;
        this.hdfsUploader = hdfsUploader;
        this.config = config;
    }

    @Retryable(value = Exception.class, maxAttempts = 1)
    public void processCsvFiles() throws Exception {
        List<String> urls = List.of(
                "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD",
                "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
        );

        int count = 1;
        for (String url : urls) {
            String hdfsPath = config.targetPath + "file_" + count++ + ".csv";
            logger.info("Processing file: {}, Target HDFS: {}", url, hdfsPath);
            InputStream csv = csvDownloader.download(url);
            hdfsUploader.upload(hdfsPath, csv);
        }
    }
}

