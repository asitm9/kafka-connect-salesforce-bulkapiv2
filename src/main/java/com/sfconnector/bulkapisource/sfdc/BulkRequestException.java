package com.sfconnector.bulkapisource.sfdc;

import java.util.List;
import java.util.stream.Collectors;

import com.sfconnector.bulkapisource.response.ErrorResponse;



public class BulkRequestException extends RuntimeException {

    private List<ErrorResponse> errors;

    public BulkRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public BulkRequestException(String message) {
        super(message);
    }

    public BulkRequestException(Exception cause) {
        super(cause);
    }

    public BulkRequestException(List<ErrorResponse> errors) {
        super(errors.stream().map(e -> e.toString()).collect(Collectors.joining(", ", "[ ", " ]")));
    }
}