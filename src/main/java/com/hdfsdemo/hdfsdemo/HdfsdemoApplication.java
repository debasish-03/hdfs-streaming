package com.hdfsdemo.hdfsdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@SpringBootApplication
@EnableAspectJAutoProxy
public class HdfsdemoApplication implements CommandLineRunner {

	private final HdfsUploaderService uploaderService;

	public HdfsdemoApplication(HdfsUploaderService uploaderService) {
		this.uploaderService = uploaderService;
	}

	public static void main(String[] args) {
		SpringApplication.run(HdfsdemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		// uploaderService.downloadAndUploadCsv();
		uploaderService.downloadAndUploadMultipleCsvs();
	}
}