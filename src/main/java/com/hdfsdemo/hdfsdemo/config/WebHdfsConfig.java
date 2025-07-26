package com.hdfsdemo.hdfsdemo.config;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebHdfsConfig {

    @Value("${csv.downloadUrl}")
    public String downloadUrl;

    @Value("${hdfs.host}")
    public String hdfsHost;

    @Value("${hdfs.port}")
    public int hdfsPort;

    @Value("${hdfs.user}")
    public String hdfsUser;

    @Value("${hdfs.targetPath}")
    public String targetPath;

    public String getWebHdfsUrl() {
        return String.format("http://%s:%d/webhdfs/v1%s?op=CREATE&user.name=%s&overwrite=true",
                hdfsHost, hdfsPort, targetPath, hdfsUser);
    }

    public String getWebHdfsBaseUrl() {
        return String.format("http://%s:%d/webhdfs/v1",
                hdfsHost, hdfsPort);
    }
}
