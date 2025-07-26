package com.hdfsdemo.hdfsdemo.uploader;


import java.io.InputStream;

public interface HdfsUploader {
    void upload(String hdfsPath, InputStream dataStream) throws Exception;
}
