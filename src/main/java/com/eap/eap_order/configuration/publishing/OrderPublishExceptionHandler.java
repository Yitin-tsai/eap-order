package com.eap.eap_order.configuration.publishing;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class OrderPublishExceptionHandler {

    @ExceptionHandler(OrderPublishException.class)
    public ResponseEntity<Map<String, Object>> handle(OrderPublishException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "5")
                .body(Map.of(
                        "error", "ORDER_PUBLISH_UNAVAILABLE",
                        "message", exception.getMessage(),
                        "retryAfterSeconds", 5
                ));
    }
}
