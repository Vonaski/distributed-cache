package com.iksanov.distributedcache.proxy.controller;

import com.iksanov.distributedcache.common.exception.CacheConnectionException;
import com.iksanov.distributedcache.common.exception.InvalidCacheRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(InvalidCacheRequestException.class)
    public ResponseEntity<Map<String, String>> handleInvalidRequest(InvalidCacheRequestException e) {
        log.warn("Invalid request: {}", e.getMessage());
        return ResponseEntity
                .badRequest()
                .body(Map.of("error", "Invalid request", "message", e.getMessage()));
    }

    @ExceptionHandler(CacheConnectionException.class)
    public ResponseEntity<Map<String, String>> handleConnectionError(CacheConnectionException e) {
        log.error("Cache connection error: {}", e.getMessage());
        return ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "Cache unavailable", "message", e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleGenericError(Exception e) {
        log.error("Unexpected error", e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error", "message", "An unexpected error occurred"));
    }
}