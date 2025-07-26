package com.hdfsdemo.hdfsdemo.controller;


import com.hdfsdemo.hdfsdemo.service.CsvHdfsService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/hdfs")
public class HdfsController {

    private final CsvHdfsService service;

    public HdfsController(CsvHdfsService service) {
        this.service = service;
    }

    @PostMapping("/upload")
    public String uploadCsvs() throws Exception {
        service.processCsvFiles();
        return "Upload triggered!";
    }
}
