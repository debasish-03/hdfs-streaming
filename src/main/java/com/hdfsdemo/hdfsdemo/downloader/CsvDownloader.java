package com.hdfsdemo.hdfsdemo.downloader;


import java.io.InputStream;

public interface CsvDownloader {
    InputStream download(String url) throws Exception;
}

