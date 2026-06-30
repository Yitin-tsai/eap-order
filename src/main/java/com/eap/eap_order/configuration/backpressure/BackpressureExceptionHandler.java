package com.eap.eap_order.configuration.backpressure;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class BackpressureExceptionHandler {

    @ExceptionHandler(BackpressureRejectedException.class)
    public ResponseEntity<Map<String, Object>> handle(BackpressureRejectedException exception) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", "ORDER_BACKPRESSURE");
        body.put("level", exception.getLevel().name());
        body.put("message", exception.getMessage());
        body.put("queueDepth", exception.getQueueDepth());
        body.put("retryAfterSeconds", exception.getRetryAfterSeconds());

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(exception.getRetryAfterSeconds()))
                .body(body);
    }
}
