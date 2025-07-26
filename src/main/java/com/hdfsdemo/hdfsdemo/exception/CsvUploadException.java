package com.hdfsdemo.hdfsdemo.exception;


public class CsvUploadException extends RuntimeException {
    public CsvUploadException(String message) {
        super(message);
    }

    public CsvUploadException(String message, Throwable cause) {
        super(message, cause);
    }
}

